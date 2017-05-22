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
        &self,
        years: Range<usize>,
        acs_estimates: &[Estimate],
        ) -> Result<()>
    {
        use Estimate::*;

        // Prep db
        self.db_client.execute(
            "CREATE TABLE IF NOT EXISTS acs_tables (
                id INTEGER PRIMARY KEY ASC,
                prefix TEXT NOT NULL,
                table_id TEXT NOT NULL,
                suffix TEXT,
                label TEXT NOT NULL
            )", &[])?;

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
            self.db_client.execute(
                "INSERT INTO acs_tables (
                    prefix,
                    table_id,
                    suffix,
                    label
                ) VALUES (
                    ?1,
                    ?2,
                    ?3,
                    ?4,
                )",
                &[
                    &code.prefix,
                    &code.table_id,
                    &code.suffix,
                    &label,
                ],
            ).unwrap();
        }

        Ok(())
    }

    pub fn refresh_acs_combination(
        &self,
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
        &self,
        year: usize,
        estimate: &Estimate,
        vars_data: &str,
        table_map: &mut HashMap<TableCode, String>,
        ) -> Result<()>
    {
        let data = json::parse(&vars_data)
            .chain_err(|| "error parsing json response")?;

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
            let variable_code = parse_variable_code(acs_var_str.as_bytes())
                .to_result()
                .chain_err(|| format!("Error parsing variable {}", acs_var_str))?;
            // TODO Think about setting up 2 tables,
            // one for tables and one for col
            let variable = Variable {
                code: variable_code,
                label: acs_info["label"].to_string(),
            };
            //println!("{:?}", variable);

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

        println!("{}", count);

        Ok(())
    }
}
