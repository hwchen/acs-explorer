use acs::*;
use error::*;
use json;
use nom::{alpha, digit, rest, space};
use reqwest;
use reqwest::{StatusCode, Url};
use rusqlite;
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::ops::Range;
use std::path::PathBuf;
use std::str;

// TODO split fetch from processing of data? for dev, this wil
// reduce churn, reading from files instead of from api.

const CENSUS_URL_BASE: &str = "https://api.census.gov/data/";
const VARS_URL: &str = "variables.json";

pub struct Explorer {
    http_client: reqwest::Client,
    db_client: rusqlite::Connection,
    census_url_base: Url,
    acs_key: String,
}

impl Explorer {
    pub fn new(
        acs_key: String,
        db_path: PathBuf,
        ) -> Result<Self>
    {
        let url = Url::parse(CENSUS_URL_BASE)?;

        Ok(Explorer {
            http_client: reqwest::Client::new()?,
            census_url_base: url,
            db_client: rusqlite::Connection::open(db_path)?,
            acs_key: acs_key,
        })
    }

    pub fn refresh(
        &mut self,
        years: Range<usize>,
        acs_estimates: &[Estimate],
        ) -> Result<()>
    {
        use Estimate::*;

        // Prep db
        self.db_client.execute_batch(
            "
            DROP TABLE IF EXISTS acs_tables;
            CREATE TABLE acs_tables (
                id INTEGER PRIMARY KEY ASC,
                prefix TEXT NOT NULL,
                table_id TEXT NOT NULL,
                suffix TEXT,
                label TEXT NOT NULL
            );
            DROP TABLE IF EXISTS acs_vars;
            CREATE TABLE acs_vars (
                id INTEGER PRIMARY KEY ASC,
                prefix TEXT NOT NULL,
                table_id TEXT NOT NULL,
                suffix TEXT,
                column_id TEXT NOT NULL,
                var_type TEXT NOT NULL,
                estimate TEXT NOT NULL,
                year INTEGER NOT NULL,
                label TEXT NOT NULL
            );
            ",
        ).chain_err(|| "Error prepping db")?;

        let mut table_map = HashMap::new();

        for year in years {
            for acs_est in acs_estimates {
                self.refresh_acs_combination(
                    year,
                    &acs_est,
                    &mut table_map,
                )?;
            }
        }

        for (code, label) in table_map.iter() {
            let mut insert = self.db_client.prepare_cached(
                "INSERT INTO acs_tables (
                    prefix,
                    table_id,
                    suffix,
                    label
                ) VALUES (
                    ?1,
                    ?2,
                    ?3,
                    ?4
                )"
            ).chain_err(|| "Error preparing acs_tables insert")?;

            insert.execute(
                &[
                    &code.prefix,
                    &code.table_id,
                    &code.suffix,
                    label,
                ]
            ).chain_err(|| "Error executing acs_tables insert")?;
        }

        Ok(())
    }

    pub fn refresh_acs_combination(
        &mut self,
        year: usize,
        acs_est: &Estimate,
        table_map: &mut HashMap<TableCode, String>,
        ) -> Result<()>
    {
        // TODO check year
        let acs_vars_data = self.fetch_acs_combination(year, acs_est)?;
        self.process_acs_vars_data(
            year,
            acs_est,
            &acs_vars_data,
            table_map,
        )
    }

    fn fetch_acs_combination(
        &self,
        year: usize,
        acs_est: &Estimate
        ) -> Result<String>
    {
        // TODO check year
        let mut year = year.to_string();
        year.push_str("/");

        let url = Url::parse(CENSUS_URL_BASE)?;
        let url = url.join(&year)?.join(acs_est.url_frag())?.join(VARS_URL)?;

        let mut resp = self.http_client.get(url).send()?;

        let mut buf = String::new();

        if let StatusCode::Ok =  *resp.status() {
            resp.read_to_string(&mut buf)?;
            Ok(buf)
        } else {
            Err(format!("Error fetching from census api: {}", resp.status()).into())
        }
    }

    // TODO at end of dev, make this private
    pub fn process_acs_vars_data(
        &mut self,
        year: usize,
        estimate: &Estimate,
        vars_data: &str,
        table_map: &mut HashMap<TableCode, String>,
        ) -> Result<()>
    {
        let data = json::parse(&vars_data)
            .chain_err(|| "error parsing json response")?;

        // 300s for 40976 vars before transaction.
        // Now 9s
        let db_tx = self.db_client.transaction()?;

        let mut count = 0;
        for (acs_var, acs_info) in data["variables"].entries() {
            let acs_var_str = acs_var.to_string();
            // Look for variable names (which have a '_' in them)
            if acs_var_str.split("_").count() != 2 {
                continue;
            }

            // currently panic on incomplete.
            // to_full_result() doesn't, but returns IError which
            // doesn't implement Error
            let code = parse_variable_code(acs_var_str.as_bytes())
                .to_result()
                .chain_err(|| format!("Error parsing variable {}", acs_var_str))?;

            // Write variable
            //TODO Should I move prep outside loop? It currently uses
            // a cached handle anyways.
            let mut insert = db_tx.prepare_cached(
                "INSERT INTO acs_vars (
                    prefix,
                    table_id,
                    suffix,
                    column_id,
                    var_type,
                    estimate,
                    year,
                    label
                ) VALUES (
                    ?1,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    ?6,
                    ?7,
                    ?8
                )"
            ).chain_err(|| "Error preparing acs_vars insert")?;

            insert.execute(
                &[
                    &code.table_code.prefix,
                    &code.table_code.table_id,
                    &code.table_code.suffix,
                    &code.column_id,
                    &code.var_type,
                    estimate,
                    &(year as u32),
                    &acs_info["label"].to_string(),
                ]
            ).chain_err(|| "Error execuring acs_vars insert")?;

            // Read table into table_map for later writing to db
            let table_str = acs_info["concept"].to_string();
            let table_record = parse_table_record(table_str.as_bytes())
                .to_result()
                .chain_err(|| format!("Error parsing table str {}", table_str))?;

            if let None = table_map.get(&table_record.code) {
                table_map.insert(
                    table_record.code,
                    table_record.label,
                );
            };

            count += 1;
        }

        db_tx.commit()?;

        println!("{}-{}: {} vars", estimate, year, count);

        Ok(())
    }
}
