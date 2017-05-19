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

pub mod census;
pub mod error;

use error::*;
use reqwest::{StatusCode, Url};
use std::io::Read;
use std::path::PathBuf;

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

    pub fn refresh(&self) -> Result<()> {
        // TODO un-hardcode
        for year in 2009..2015 {
            for acs_est in &["acs1/", "acs5/"] {
                self.refresh_acs_vars(year, acs_est)?;
            }
        }

        Ok(())
    }

    pub fn refresh_acs_vars(&self, year: usize, acs_est: &str) -> Result<()> {
        let mut year = year.to_string();
        year.push_str("/");

        let url = Url::parse(CENSUS_URL_BASE)?;
        let url = url.join(&year)?.join(&acs_est)?.join(VARS_URL)?;

        let mut resp = self.http_client.get(url).send()?;

        match *resp.status() {
            StatusCode::Ok => {
                let mut buf = String::new();
                resp.read_to_string(&mut buf);
                let data = json::parse(&buf).chain_err(|| "error parsing json response")?;

                let mut count = 0;
                for (acs_var, acs_info) in data["variables"].entries() {
                    let acs_var = acs_var.to_string();
                    // Look for variable names (which have a '_' in them)
                    if acs_var.split("_").count() != 2 {
                        continue;
                    }
                    // TODO parse an indicator var
                    let label = acs_var["label"];
                    let concept = acs_var["concept"];

                    count += 1;
                }
                println!("{}", count);
            },
            _ => {
                println!("No vars for {}, {}", year, acs_est);
            }
        }
        Ok(())
    }
}

named!(parse_variable<&[u8], VariableCode>,
    do_parse!(
        prefix: parse_prefix >>
        id: parse_id >>
        suffix: parse_suffix >>

        (VariableCode {
            prefix: prefix,
            variable_id: id,
            suffix: suffix,
        })
    )
);

named!(parse_prefix<&[u8], VariablePrefix>,
    do_parse!(
        prefix: alt!(tag!("B") | tag!("C")) >>

        (match prefix {
            b"B" => VariablePrefix::B,
            b"C" => VariablePrefix::C,
        }
        )
    )
);

named!(parse_id<&[u8], String>,
    map_res!(
        take!(5),
        |id| String::from_utf8(id.to_string())
    )
);

named!(parse_suffix<&[u8], String>,
    map_res!(
        take_until!(b"_"),
        |id| String::from_utf8(id.to_string())
    )
);

// this is what gets stored in the database
// Not for public access?
struct VariableRecord {
    variable: Variable,
    estimate: Estimate,
    year: usize, // I just use one big table, denormalized
}

pub struct Variable {
    label: String, // Encodes Hierarchy
    indicator: Indicator,
}

pub struct VariableCode {
    prefix: IndicatorPrefix,
    variable_id: String,
    suffix: String, // should be limited to upper-case letters?
}

pub enum VariablePrefix {
    B,
    C,
}

pub enum Estimate {
    OneYear,
    FiveYear,
}

pub enum VariableType {
    MarginOfError,
    Value,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
