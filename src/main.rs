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

extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate json;
#[macro_use]
extern crate nom;
extern crate reqwest;
extern crate rusqlite;

mod acs;
mod census;
mod error;
mod explorer;

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

    // Setup for database
    let mut db_path = PathBuf::from(ACS_DIR);
    db_path.push(DB_FILE);

    env::set_current_dir(env::home_dir().ok_or("No home dir found!")?)?;

    fs::create_dir_all(ACS_DIR)?;

    // Instantiate Explorer and go!
    let explorer = Explorer::new(
        "acs_key".to_owned(),
        PathBuf::from(&db_path),
    ).unwrap();

    let mut table_map = ::std::collections::HashMap::new();
    //explorer.refresh_acs_combination(2009, &Estimate::FiveYear, &mut table_map)?;

    //tmp just reading from file instead of fetching
    //----------------------------------------------
    use std::fs::File;
    use std::io::Read;
    let mut f = File::open("/home/hwchen/projects/rust/acs-explorer/temp-vars.json")?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;
    explorer.process_acs_vars_data(2009, &Estimate::FiveYear, &buf, &mut table_map)?;
    //----------------------------------------------

    for entry in table_map.iter() {
        println!("{:?}", entry);
    }

    Ok(())
}
