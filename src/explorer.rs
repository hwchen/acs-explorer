use acs::*;
use error::*;

use json;
use reqwest;
use reqwest::{StatusCode, Url};
use rusqlite;
use std::collections::HashMap;
use std::io::Read;
use std::ops::Range;
use std::path::PathBuf;
use std::str;
use time;

// Timings (includ an api call!):
// - using no transactions and cached sql handle on tables and vars: 300s
// - adding transaction for vars only: 9s
// - adding transaction for tables (on top of vars): 2.8s
// looks like whether using prepare or prepare_cached, time is about same
// (perf should be same, semantics are more ergonomic for maintenance using
// prepare_cached because it allows the prep statement to be next to variables
//
// pragmas don't seem to make much difference here.
//
// A quick crude timing using time crate suggests that the fetch operation
// takes around 3 seconds, which leaves the processing at .40 seconds.
//
// Wishlist
// - progress bar
// - start search for table id (clap) which returns a table of
//   all related variables, along with their years and estimates.
//   (maybe use a flag to do per year or per estimate or per var)
// - Then search for all the vars in one table.
// Both these searches use table_id. What to call them?
//
// fuzzy search? (on label only)
// should I just make both tables into one?
//
// print stats after a refresh
//
// TODO next I want to know the years and estimates of each table. Never search
// by var.
// - format table (for <=2 and >2)
// - sqlite composite index and foreign key, merge tables?
// - separate Command from Option in cli! return a tuple of both. Then
//   command can be sent in, and options simply parsed.
// - rename commands: describe, fetch, find, --table --label
// - fix query order to match index
// - reimplement the ToSql to be a more compact format

const CENSUS_URL_BASE: &str = "https://api.census.gov/data/";
const VARS_URL: &str = "variables.json";

pub struct Explorer {
    http_client: reqwest::Client,
    db_client: rusqlite::Connection,
    acs_key: String,
}

impl Explorer {
    pub fn new(
        acs_key: String,
        db_path: PathBuf,
        ) -> Result<Self>
    {
        Ok(Explorer {
            http_client: reqwest::Client::new()?,
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

        self.db_client.execute_batch("PRAGMA synchronous = OFF")
            .chain_err(|| "Error turning synchronous off")?;

        self.db_client.execute_batch("PRAGMA journal_mode = MEMORY")
            .chain_err(|| "Error switching journal mode to Memory")?;

        let mut table_map = HashMap::new();

        for year in years {
            for acs_est in acs_estimates {
                match self.refresh_acs_combination(
                    year,
                    &acs_est,
                    &mut table_map,
                ) {
                    Ok(_) => println!("completed refresh {}-{}", year, acs_est),
                    Err(err) => println!("no refresh {}-{}: {}", year, acs_est, err),
                }
            }
        }

        let db_tx = self.db_client.transaction()?;

        for (code, label) in table_map.iter() {
            let mut insert = db_tx.prepare_cached(
                "INSERT INTO acs_tables (
                    prefix,
                    table_id,
                    suffix,
                    label
                ) VALUES (
                    ?1, ?2, ?3, ?4
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

        db_tx.commit()?;

        // TODO print stats after a refresh

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
        let start = time::precise_time_s();
        let acs_vars_data = self.fetch_acs_combination(year, acs_est)?;
        let end = time::precise_time_s();
        println!("Fetch time for {}-{}: {}", year, acs_est, end - start);
        let start = time::precise_time_s();
        let res = self.process_acs_vars_data(
            year,
            acs_est,
            &acs_vars_data,
            table_map,
        );
        let end = time::precise_time_s();
        println!("Process time for {}-{}: {}", year, acs_est, end - start);

        res
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
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
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

    pub fn query_by_table_id(
        &mut self,
        prefix: Option<TablePrefix>,
        table_id: String,
        suffix: Option<String>
        ) -> Result<Vec<TableRecord>>
    {
        if let Some(prefix) = prefix {
            if let Some(suffix) = suffix {
                // has both prefix and suffix
                let mut query = self.db_client.prepare(
                "SELECT prefix, table_id, suffix, label
                    FROM acs_tables
                    WHERE prefix = ?1
                        AND suffix = ?2
                        AND table_id = ?3
                ")?;
                let records = query.query_map(&[&prefix, &suffix, &table_id], |row| {
                    TableRecord {
                        code: TableCode {
                            prefix: row.get(0),
                            table_id: row.get(1),
                            suffix: row.get(2),
                        },
                        label: row.get(3),
                    }
                })?;

                let mut res = Vec::new();
                for record in records {
                    res.push(record?);
                }
                Ok(res)
            } else {
                // has prefix but no suffix
                let mut query = self.db_client.prepare(
                "SELECT prefix, table_id, suffix, label
                    FROM acs_tables
                    WHERE prefix = ?1
                        AND table_id = ?2
                ")?;
                let records = query.query_map(&[&prefix, &table_id], |row| {
                    TableRecord {
                        code: TableCode {
                            prefix: row.get(0),
                            table_id: row.get(1),
                            suffix: row.get(2),
                        },
                        label: row.get(3),
                    }
                })?;

                let mut res = Vec::new();
                for record in records {
                    res.push(record?);
                }
                Ok(res)
            }
        } else {
            if let Some(suffix) = suffix {
                // has suffix but no prefix
                let mut query = self.db_client.prepare(
                "SELECT prefix, table_id, suffix, label
                    FROM acs_tables
                    WHERE suffix = ?1
                        AND table_id = ?2
                ")?;
                let records = query.query_map(&[&suffix, &table_id], |row| {
                    TableRecord {
                        code: TableCode {
                            prefix: row.get(0),
                            table_id: row.get(1),
                            suffix: row.get(2),
                        },
                        label: row.get(3),
                    }
                })?;

                let mut res = Vec::new();
                for record in records {
                    res.push(record?);
                }
                Ok(res)
            } else {
                // has no suffix and no prefix
                let mut query = self.db_client.prepare(
                "SELECT prefix, table_id, suffix, label
                    FROM acs_tables
                    WHERE table_id = ?1
                ")?;

                let records = query.query_map(&[&table_id], |row| {
                    TableRecord {
                        code: TableCode {
                            prefix: row.get(0),
                            table_id: row.get(1),
                            suffix: row.get(2),
                        },
                        label: row.get(3),
                    }
                })?;

                let mut res = Vec::new();
                for record in records {
                    res.push(record?);
                }
                Ok(res)
            }
        }
    }

    pub fn describe_table(
        &mut self,
        prefix: TablePrefix,
        table_id: String,
        suffix: Option<String>,
        ) -> Result<Vec<VariableRecord>>
    {
        let sql_str = "
            SELECT t.prefix, t.table_id, t.suffix, t.label,
                v.column_id, v.var_type, v.year, v.estimate, v.label
            from acs_tables t left join acs_vars v
                on (t.table_id = v.table_id and t.prefix = v.prefix)
            where t.table_id = ?1 and t.prefix = ?2 and t.suffix
        ";
        let sql_str = if suffix.is_none() {
            format!("{} {};", sql_str, "is null")
        } else {
            format!("{} {};", sql_str, "= ?3")
        };

        let mut query = self.db_client.prepare(&sql_str)?;

        // duplication just to handle putting in the right number of
        // args
        if !suffix.is_none() {
            let vars = query.query_map(&[&table_id, &prefix, &suffix], |row| {
                VariableRecord {
                    variable: Variable {
                        label: row.get(8),
                        code: VariableCode {
                            table_code: TableCode {
                                prefix: row.get(0),
                                table_id: row.get(1),
                                suffix: row.get(2),
                            },
                            column_id: row.get(4),
                            var_type: row.get(5),
                        }
                    },
                    estimate: row.get(7),
                    year: row.get(6),
                }
            })?;

            let mut res = Vec::new();
            for var in vars {
                res.push(var?);
            }
            Ok(res)
        } else {
            let vars = query.query_map(&[&table_id, &prefix], |row| {
                VariableRecord {
                    variable: Variable {
                        label: row.get(8),
                        code: VariableCode {
                            table_code: TableCode {
                                prefix: row.get(0),
                                table_id: row.get(1),
                                suffix: row.get(2),
                            },
                            column_id: row.get(4),
                            var_type: row.get(5),
                        }
                    },
                    estimate: row.get(7),
                    year: row.get(6),
                }
            })?;

            let mut res = Vec::new();
            for var in vars {
                res.push(var?);
            }
            Ok(res)
        }

    }

}
