#![recursion_limit = "1024"]

/// ACS Explorer
///
/// Basically, using census reporter is too slow and doesn't tell
/// if a particular table is actually available in the census api.
///
/// The cli will let you check information about a table ID:
///
/// - whether there exists a B or C version
/// - what years and acs estimate (1,5) it exists in
/// - variables for that table.
/// - get data for that table (just curl)
///
/// Features:
/// - stores variables info in file (or sqlite? too heavy?)
/// - refresh variables data on command and prompt first time
/// - stored data goes into .census folder, or user-defined. (first-time prompt)
/// - read acs key from env var.
/// - fuzzy finder for tables
/// - refresh should have
///
/// For example, these endpoints:
///
/// curl -v "https://api.census.gov/data/2015/acs5/variables.json" >

#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate fst;
extern crate json;
#[macro_use]
extern crate nom;
extern crate reqwest;
extern crate rusqlite;
extern crate time;

mod acs;
mod cli;
mod census;
mod error;
mod explorer;

use cli::{cli_command, Command};
use error::*;
use explorer::Explorer;
// temp
use acs::{
    Estimate,
    format_table_name,
    format_describe_table_raw,
    format_describe_table_pretty,
    format_est_years,
    format_etl_config,
};

use std::env;
use std::fs;
use std::path::{PathBuf};
use std::process;

// file name for sqlite db acs vars store
const DB_FILE: &str = "vars.db";
const ACS_DIR: &str = ".acs-explorer";

fn main() {
    if let Err(ref err) = run() {
        println!("error: {}", err);

        for e in err.iter().skip(1) {
            println!(" cause by: {}", e);
        }

        if let Some(backtrace) = err.backtrace() {
            println!("backtrace: {:?}", backtrace);
        }

        ::std::process::exit(1);
    }
}

fn run() -> Result<()> {

    // get cli command
    let command = cli_command()
        .chain_err(|| "Error getting command")?;

    // Setup for database
    let mut db_path = PathBuf::from(ACS_DIR);
    db_path.push(DB_FILE);

    env::set_current_dir(env::home_dir().ok_or("No home dir found!")?)?;

    fs::create_dir_all(ACS_DIR)?;

    // Instantiate Explorer and go!
    let mut explorer = Explorer::new(
        "acs_key".to_owned(),
        db_path,
    ).unwrap();

    let current_year = time::now().tm_year + 1900;

    use Command::*;
    match command.command {
        Refresh => {
            println!("Refreshing...");

            let start = time::precise_time_s();
            explorer.refresh(2009..current_year as usize, &[Estimate::FiveYear, Estimate::OneYear])?;
            let end = time::precise_time_s();
            println!("Overall refresh time: {}", end - start);
        },

        FulltextSearch(search) => println!("fulltext search: {:?}", search),

        DescribeTable{ ref query, etl_config, raw} => {
            // prefix checked to be Some already, so can unwrap
            let records = explorer.describe_table(
                query.prefix.as_ref().unwrap(),
                &query.table_id,
                &query.suffix,
            )?;

            let suffix_display = match query.suffix {
                Some(ref s) => s,
                None => "",
            };

            if records.is_empty() {
                println!("Table {}{}{} not found.",
                    query.prefix.as_ref().unwrap(),
                    query.table_id,
                    suffix_display,
                );
                process::exit(0);
            }

            // from cli, etl_config and raw are guaranteed to not both be true at same time.
            let mut out = if etl_config {
                format_etl_config(current_year as u32, records)
            } else if raw {
                format_describe_table_raw(current_year as u32, records)
            } else {
                format_describe_table_pretty(current_year as u32, records)
            };

            if !(raw || etl_config) {
                out.push_str("Table Information:\n============================================\n\n");
                let table_info = explorer.query_by_table_id(
                    &query.prefix,
                    &query.table_id,
                    &query.suffix
                )?;
                if let Some(table_record) = table_info.get(0) {
                    out.push_str(&format_table_name(&table_record));
                    out.push_str("\n");
                }

                let est_years = explorer.query_est_years(
                    query.prefix.as_ref().unwrap(),
                    &query.table_id,
                    &query.suffix
                )?;
                out.push_str(&format_est_years(&est_years));
            }
            println!("{}", out);
        },
    }

    Ok(())
}
