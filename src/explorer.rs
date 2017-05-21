use error::*;
use json;
use nom::digit;
use reqwest;
use reqwest::{StatusCode, Url};
use rusqlite;
use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::str;

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
        let mut table_map = HashMap::new();

        // TODO un-hardcode
        for year in 2009..2015 {
            for acs_est in &["acs1/", "acs5/"] {
                self.refresh_acs_vars(year, acs_est, &mut table_map)?;
            }
        }

        Ok(())
    }

    pub fn refresh_acs_vars(
        &self,
        year: usize,
        acs_est: &str,
        table_map: &mut HashMap<String, String>,
        ) -> Result<()>
    {
        let mut year = year.to_string();
        year.push_str("/");

        let url = Url::parse(CENSUS_URL_BASE)?;
        let url = url.join(&year)?.join(&acs_est)?.join(VARS_URL)?;

        let mut resp = self.http_client.get(url).send()?;

        if let StatusCode::Ok =  *resp.status() {
            let mut buf = String::new();
            resp.read_to_string(&mut buf)?;
            println!("{}", buf);
            ::std::process::exit(0);

            let data = json::parse(&buf)
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
                // TODO rename vars to to include
                // table and columns, with labels.
                // Think about setting up 2 tables,
                // one for tables and one for col
                let variable = Variable {
                    variable_code: variable_code,
                    label: acs_info["label"].to_string(),
                };
                //println!("{:?}", variable);

                //table_map.get_mut(

                // TODO parse an indicator var
                //let label = acs_var["label"];
                //let concept = acs_var["concept"];

                count += 1;
            }

            println!("{}", count);

        } else {
            println!("No vars for {}, {}", year, acs_est);
        }

        Ok(())
    }
}

named!(parse_variable_code<&[u8], VariableCode>,
    do_parse!(
        prefix: parse_prefix >>
        table_id: parse_table_id >>
        suffix: parse_suffix >>
        tag!("_") >>
        column_id: parse_column_id >>
        var_type: parse_var_type >>

        ({
            let suffix = match &suffix[..] {
                "" => None,
                _ => Some(suffix),
            };

            VariableCode {
                prefix: prefix,
                table_id: table_id,
                suffix: suffix,
                column_id: column_id,
                var_type: var_type,
            }
        })
    )
);

named!(parse_prefix<&[u8], VariablePrefix>,
    do_parse!(
        prefix: alt!(tag!("B") | tag!("C")) >>

        (match prefix {
            b"B" => VariablePrefix::B,
            b"C" => VariablePrefix::C,
            _ => VariablePrefix::B, // TODO Fix error handling later
        })
    )
);

named!(parse_table_id<&[u8], String>,
    map_res!(
        take!(5),
        |id| str::from_utf8(id).map(|s| s.to_owned())
    )
);

named!(parse_suffix<&[u8], String>,
    map_res!(
        take_until!(&b"_"[..]),
        |id| str::from_utf8(id).map(|s| s.to_owned())
    )
);

named!(parse_column_id<&[u8], String>,
    map_res!(
        digit,
        |id| str::from_utf8(id).map(|s| s.to_owned())
    )
);

named!(parse_var_type<&[u8], VariableType>,
    do_parse!(
        prefix: alt!(tag!("E") | tag!("M")) >>

        (match prefix {
            b"E" => VariableType::Value,
            b"M" => VariableType::MarginOfError,
            _ => VariableType::Value, // TODO Fix error handling later
        }
        )
    )
);

// this is what gets stored in the database
// Not for public access?
#[derive(Debug, Clone, PartialEq)]
struct VariableRecord {
    variable: Variable,
    estimate: Estimate,
    year: usize, // I just use one big table, denormalized
}

#[derive(Debug, Clone, PartialEq)]
pub struct Variable {
    label: String, // Encodes Hierarchy
    variable_code: VariableCode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariableCode {
    prefix: VariablePrefix,
    table_id: String,
    suffix: Option<String>, // should be limited to upper-case letters?
    column_id: String,
    var_type: VariableType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariablePrefix {
    B,
    C,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Estimate {
    OneYear,
    FiveYear,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariableType {
    MarginOfError,
    Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::IResult;

    #[test]
    fn test_parse_variable_code() {
        let input = "B20005E_045M";
        let expected = VariableCode {
            prefix: VariablePrefix::B,
            table_id: "20005".to_owned(),
            suffix: Some("E".to_owned()),
            column_id: "045".to_owned(),
            var_type: VariableType::MarginOfError,
        };
        assert_eq!(
            parse_variable_code(input.as_bytes()),
            IResult::Done(&b""[..], expected)
        );
    }
}
