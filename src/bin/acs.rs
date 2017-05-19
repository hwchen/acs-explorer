extern crate acs_explorer;

use acs_explorer::error::*;
use acs_explorer::Explorer;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

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

    explorer.refresh_acs_vars(2009, "acs5/")?;

    Ok(())
}
