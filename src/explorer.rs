use acs::*;
use error::*;
use fulltext::*;

use json;
use reqwest;
use reqwest::{StatusCode, Url};
use rusqlite;
use std::collections::{HashMap, HashSet};
use std::fs::File;
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
// For describe queries, two lookups is faster: 0.11s for query just acs_vars,
// and 2.43s for query on join of acs_table and acs_vars
//
// Timing on etl lookup is 0.15 before indexing, 0.05 after. (with stdout)) (or now 0.01?)
//
// TODO next I want to know the years and estimates of each table. Never search
// by var.
// - rename commands: describe, fetch, find, --table --label
// - reimplement the ToSql to be a more compact format
//
// - improve formatting (prettier, showing levels better for describe)
//
// - don't change search (by table_id) right now? In the future, use a search
//   engine to give a drop-down list of choices.
//
// - Figure out how to keep hashmap in order (ordermap)
// - have switches for showing label name and est_years

const CENSUS_URL_BASE: &str = "https://api.census.gov/data/";
const VARS_URL: &str = "variables.json";

pub struct Explorer {
    http_client: reqwest::Client,
    db_client: rusqlite::Connection,
    search_index_path: PathBuf,
    acs_key: String,
}

impl Explorer {
    pub fn new(
        acs_key: String,
        db_path: PathBuf,
        search_index_path: PathBuf,
        ) -> Result<Self>
    {
        Ok(Explorer {
            http_client: reqwest::Client::new()?,
            db_client: rusqlite::Connection::open(db_path)?,
            search_index_path: search_index_path,
            acs_key: acs_key,
        })
    }

    pub fn refresh(
        &mut self,
        years: Range<usize>,
        acs_estimates: &[Estimate],
        ) -> Result<()>
    {
        // Prep search index builder
        let mut search_builder = SearchBuilder::new(
            File::create(&self.search_index_path)?
        )?;

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
                label TEXT NOT NULL
            );
            DROP TABLE IF EXISTS acs_est_years;
            CREATE TABLE acs_est_years (
                id INTEGER PRIMARY KEY ASC,
                prefix TEXT NOT NULL,
                table_id TEXT NOT NULL,
                suffix TEXT,
                estimate TEXT NOT NULL,
                year INTEGER NOT NULL
            );
            ",
        ).chain_err(|| "Error prepping db")?;

        self.db_client.execute_batch("PRAGMA synchronous = OFF")
            .chain_err(|| "Error turning synchronous off")?;

        self.db_client.execute_batch("PRAGMA journal_mode = MEMORY")
            .chain_err(|| "Error switching journal mode to Memory")?;

        let mut table_map = HashMap::new();
        let mut vars_map = HashMap::new();

        for year in years {
            for acs_est in acs_estimates {
                match self.refresh_acs_combination(
                    year,
                    &acs_est,
                    &mut table_map,
                    &mut vars_map,
                ) {
                    Ok(_) => println!("completed refresh {}-{}", year, acs_est),
                    Err(err) => println!("no refresh {}-{}: {}", year, acs_est, err),
                }
            }
        }

        {
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

                // BUILD SEARCH INDEX
                // in this small subsection
                // first break label into words
                // then insert each one as a key with the table
                // primary key as the value.
                //for word 
            }

            for (code, label) in vars_map.iter() {
                let mut insert = db_tx.prepare_cached(
                    "INSERT INTO acs_vars (
                        prefix,
                        table_id,
                        suffix,
                        column_id,
                        var_type,
                        label
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6
                    )"
                ).chain_err(|| "Error preparing acs_vars insert")?;

                insert.execute(
                    &[
                        &code.table_code.prefix,
                        &code.table_code.table_id,
                        &code.table_code.suffix,
                        &code.column_id,
                        &code.var_type,
                        label,
                    ]
                ).chain_err(|| "Error executing acs_tables insert")?;
            }

            db_tx.commit()?;
        }

        self.db_client.execute_batch("
            CREATE INDEX acs_vars_id_idx on acs_vars (table_id, prefix, suffix);
            CREATE INDEX acs_tables_id_idx on acs_tables (table_id, prefix, suffix);
            CREATE INDEX acs_tables_est_years_idx on acs_est_years (table_id, prefix, suffix);
        ").chain_err(|| "Error creating indexes")?;

        search_builder.finish()?;

        Ok(())
    }

    pub fn refresh_acs_combination(
        &mut self,
        year: usize,
        acs_est: &Estimate,
        table_map: &mut HashMap<TableCode, String>,
        vars_map: &mut HashMap<VariableCode, String>,
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
            vars_map,
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
        vars_map: &mut HashMap<VariableCode, String>,
        ) -> Result<()>
    {
        let data = json::parse(&vars_data)
            .chain_err(|| "error parsing json response")?;

        let mut est_years_set = HashSet::new();

        let db_tx = self.db_client.transaction()?;

        let mut count = 0;
        for (acs_var, acs_info) in data["variables"].entries() {
            // Read varsin into var_map for later writing into db
            // This normalizes the data, which is good for the kind
            // of fetching we'll be doing later.

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

            // put code/label in map for later writing
            // (removes duplication)
            if let None = vars_map.get(&code) {
                vars_map.insert(code, acs_info["label"].to_string());
            };

            // parse table code
            // Read table into table_map for later writing to db
            // Read table code into est_vars_map (local) for writing
            // into db at end of this fn.
            let table_str = acs_info["concept"].to_string();
            let table_record = parse_table_record(table_str.as_bytes())
                .to_result()
                .chain_err(|| format!("Error parsing table str {}", table_str))?;

            // Can I get rid of this clone? Probably, but
            // more complicated. The HashSet only lives to
            // the end of this fn.
            est_years_set.insert(table_record.code.clone());

            // read into table_map for later writing to db
            if let None = table_map.get(&table_record.code) {
                table_map.insert(
                    table_record.code,
                    table_record.label,
                );
            };

            count += 1;
        }

        // now that all codes are found for this year/est combo,
        // write before moving onto next combo.
        for code in est_years_set {
            // write years and est per table
            let mut insert = db_tx.prepare_cached(
                "INSERT INTO acs_est_years (
                    prefix,
                    table_id,
                    suffix,
                    estimate,
                    year
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5
                )"
            ).chain_err(|| "Error preparing acs_est_years insert")?;

            insert.execute(
                &[
                    &code.prefix,
                    &code.table_id,
                    &code.suffix,
                    estimate,
                    &(year as u32),
                ]
            ).chain_err(|| "Error executing acs_est_years insert")?;
        }

        db_tx.commit()?;

        println!("{}-{}: {} vars", estimate, year, count);

        Ok(())
    }

    pub fn query_by_table_id(
        &mut self,
        prefix: &Option<TablePrefix>,
        table_id: &str,
        suffix: &Option<String>
        ) -> Result<Vec<TableRecord>>
    {
        if let Some(ref prefix) = *prefix {
            if let Some(ref suffix) = *suffix {
                // has both prefix and suffix
                let mut query = self.db_client.prepare(
                "SELECT prefix, table_id, suffix, label
                    FROM acs_tables
                    WHERE prefix = ?1
                        AND suffix = ?2
                        AND table_id = ?3
                ")?;
                let records = query.query_map(&[prefix, suffix, &table_id], |row| {
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
                let records = query.query_map(&[prefix, &table_id], |row| {
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
            if let Some(ref suffix) = *suffix {
                // has suffix but no prefix
                let mut query = self.db_client.prepare(
                "SELECT prefix, table_id, suffix, label
                    FROM acs_tables
                    WHERE suffix = ?1
                        AND table_id = ?2
                ")?;
                let records = query.query_map(&[suffix, &table_id], |row| {
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
        prefix: &TablePrefix,
        table_id: &str,
        suffix: &Option<String>,
        ) -> Result<Vec<VariableRecord>>
    {
        let sql_str = "
            SELECT prefix, table_id, suffix,
                column_id, var_type, label
            from acs_vars
            where table_id = ?1 and prefix = ?2 and suffix
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
            let start = time::precise_time_s();
            let vars = query.query_map(&[&table_id, prefix, suffix], |row| {
                VariableRecord {
                    label: row.get(5),
                    code: VariableCode {
                        table_code: TableCode {
                            prefix: row.get(0),
                            table_id: row.get(1),
                            suffix: row.get(2),
                        },
                        column_id: row.get(3),
                        var_type: row.get(4),
                    }
                }
            })?;

            let mut query = self.db_client.prepare("
                select count(*)
                from acs_vars
                where table_id = ?1 and prefix = ?2 and suffix is null
            ")?;

            let count: u32 = query.query_row(&[&table_id, prefix], |row| {
                row.get(0)
            })?;

            let mut res = Vec::with_capacity(count as usize);
            // allocating capacity beforehand saves maybe .01s? .023 -> .014
            //let mut res = Vec::new();
            for var in vars {
                res.push(var?);
            }

            let end = time::precise_time_s();
            //println!("query time and collecting for vars: {}", end - start);
            Ok(res)
        } else {
            let start = time::precise_time_s();
            let vars = query.query_map(&[&table_id, prefix], |row| {
                VariableRecord {
                    label: row.get(5),
                    code: VariableCode {
                        table_code: TableCode {
                            prefix: row.get(0),
                            table_id: row.get(1),
                            suffix: row.get(2),
                        },
                        column_id: row.get(3),
                        var_type: row.get(4),
                    }
                }
            })?;

            let mut query = self.db_client.prepare("
                select count(*)
                from acs_vars
                where table_id = ?1 and prefix = ?2 and suffix is null
            ")?;

            let count: u32 = query.query_row(&[&table_id, prefix], |row| {
                row.get(0)
            })?;

            let mut res = Vec::with_capacity(count as usize);
            // allocating capacity beforehand saves maybe .01s? .023 -> .014
            //let mut res = Vec::new();
            for var in vars {
                res.push(var?);
            }
            let end = time::precise_time_s();
            //println!("query time and collecting for vars: {}", end - start);
            Ok(res)
        }

    }

    pub fn query_est_years(
        &mut self,
        prefix: &TablePrefix,
        table_id: &str,
        suffix: &Option<String>,
        ) -> Result<HashMap<Estimate, Vec<u32>>>
    {
        let sql_str = "
            SELECT estimate, year
            from acs_est_years
            where table_id = ?1 and prefix = ?2 and suffix
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
            let mut res = HashMap::new();
            let start = time::precise_time_s();

            let rows = query.query_map(&[&table_id, prefix, suffix], |row| {
                (row.get(0), row.get(1))
            })?;

            for row in rows {
                let row = row?;
                res.entry(row.0).or_insert(Vec::new()).push(row.1);
            }

            let end = time::precise_time_s();
            //println!("query time and collecting for est years: {}", end - start);
            Ok(res)
        } else {
            let mut res = HashMap::new();
            let start = time::precise_time_s();

            let rows = query.query_map(&[&table_id, prefix, suffix], |row| {
                (row.get(0), row.get(1))
            })?;

            for row in rows {
                let row = row?;
                res.entry(row.0).or_insert(Vec::new()).push(row.1);
            }

            let end = time::precise_time_s();
            //println!("query time and collecting for est years: {}", end - start);
            Ok(res)
        }

    }
}
