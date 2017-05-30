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

use cli::{cli_command, Command, FindTableQuery, TableIdQuery};
use error::*;
use explorer::Explorer;
// temp
use acs::Estimate;

use std::env;
use std::fs;
use std::path::{PathBuf};

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
        PathBuf::from(&db_path),
    ).unwrap();

    use Command::*;
    use FindTableQuery::*;
    match command.command {
        Refresh => {
            let current_year = time::now().tm_year as usize + 1900;

            let start = time::precise_time_s();
            explorer.refresh(2009..current_year, &[Estimate::FiveYear, Estimate::OneYear])?;
            let end = time::precise_time_s();
            println!("Overall refresh time: {}", end - start);
        },
        FindTable(ByTableId(TableIdQuery {prefix, table_id, suffix})) => {
            println!("{:?}, {}, {:?}", prefix, table_id, suffix);
            let records = explorer.query_by_table_id(prefix, table_id, suffix)?;
            for (i, record) in records.iter().enumerate() {
                println!("{}, {:?}", i, record);
            }
        },
        FindTable(ByLabel(s)) => println!("label query: {:?}", s),
        DescribeTable => println!("a variable query"),
        FetchTable => println!("a variable query"),
    }

    Ok(())
}
