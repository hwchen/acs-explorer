use error::*;
use nom::{alpha, digit, rest, space, IResult};
use rusqlite;
use rusqlite::types::{ToSql, ToSqlOutput};
use std::fmt;
use std::str;

pub fn parse_variable_code(input: &[u8]) -> IResult<&[u8], VariableCode> {
    do_parse!(input,
        table_code: parse_table_code >>
        tag!("_") >>
        column_id: parse_column_id >>
        var_type: parse_var_type >>

        (VariableCode {
                table_code: table_code,
                column_id: column_id,
                var_type: var_type,
        })
    )
}

pub fn parse_table_record(input: &[u8]) -> IResult<&[u8], TableRecord> {
    do_parse!(input,
        table_code: parse_table_code >>
        tag!(".") >>
        space >>
        label: map_res!(
            rest,
            |id| str::from_utf8(id).map(|s| s.to_owned())
            ) >>

        (TableRecord {
            code: table_code,
            label: label,
        })
    )
}

named!(parse_table_code<&[u8], TableCode>,
    do_parse!(
        prefix: parse_prefix >>
        table_id: parse_table_id >>
        suffix: parse_suffix >>

        (TableCode {
                prefix: prefix,
                table_id: table_id,
                suffix: suffix,
        })
    )
);

named!(parse_prefix<&[u8], TablePrefix>,
    do_parse!(
        prefix: alt!(tag!("B") | tag!("C")) >>

        (match prefix {
            b"B" => TablePrefix::B,
            b"C" => TablePrefix::C,
            _ => TablePrefix::B, // TODO Fix error handling later
        })
    )
);

pub fn parse_table_id(input: &[u8]) -> IResult<&[u8], String> {
    map_res!(input,
        digit,
        |id| str::from_utf8(id).map(|s| s.to_owned())
    )
}

pub fn parse_suffix(input: &[u8]) -> IResult<&[u8], Option<String>> {
    opt!(input, map_res!(
        alpha,
        |suffix| {
            str::from_utf8(suffix)
                .map(|s| s.to_owned())
        }
    ))
}

named!(parse_column_id<&[u8], String>,
    map_res!(
        digit,
        |id| str::from_utf8(id).map(|s| s.to_owned())
    )
);

named!(parse_var_type<&[u8], VariableType>,
    map_res!(
        alt!(tag!("E") | tag!("M")),
        match_var_type
    )
);

fn match_var_type(input: &[u8]) -> Result<VariableType> {
    match input {
        b"E" => Ok(VariableType::Value),
        b"M" => Ok(VariableType::MarginOfError),
        v => {
            let v = str::from_utf8(v)
                .chain_err(|| "non utf8 value for VariableType")?;
            Err(format!("Unrecognized value for VariableType: {}", v).into())
        }
    }
}

// this is what gets stored in the database
// Not for public access?
#[derive(Debug, Clone, PartialEq)]
pub struct VariableRecord {
    variable: Variable,
    estimate: Estimate,
    year: usize, // I just use one big table, denormalized
}

#[derive(Debug, Clone, PartialEq)]
pub struct Variable {
    pub label: String, // Encodes Hierarchy
    pub code: VariableCode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariableCode {
    pub table_code: TableCode,
    pub column_id: String,
    pub var_type: VariableType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRecord {
    pub code: TableCode,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableCode {
    pub prefix: TablePrefix,
    pub table_id: String,
    pub suffix: Option<String>, // should be limited to upper-case letters?
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TablePrefix {
    B,
    C,
}

impl ToSql for TablePrefix {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl ToString for TablePrefix {
    fn to_string(&self) -> String {
        match *self {
            TablePrefix::B => "B".to_owned(),
            TablePrefix::C => "C".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariableType {
    MarginOfError,
    Value,
}

impl ToSql for VariableType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl ToString for VariableType {
    fn to_string(&self) -> String {
        match *self {
            VariableType::MarginOfError => "M".to_owned(),
            VariableType::Value => "E".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Estimate {
    OneYear,
    FiveYear,
}

impl Estimate {
    pub fn url_frag(&self) -> &str {
        const ACS_1_FRAG: &str = "acs1/";
        const ACS_5_FRAG: &str = "acs5/";

        match *self {
            Estimate::OneYear => ACS_1_FRAG,
            Estimate::FiveYear => ACS_5_FRAG,
        }
    }
}

impl fmt::Display for Estimate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Estimate::OneYear => write!(f, "ACS 1-year estimate"),
            Estimate::FiveYear => write!(f, "ACS 5-year estimate"),
        }
    }
}

impl ToSql for Estimate {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

//impl ToString for Estimate {
//    fn to_string(&self) -> String {
//        match *self {
//            Estimate::OneYear => "acs1".to_owned(),
//            Estimate::FiveYear => "acs5".to_owned(),
//        }
//    }
//}


#[cfg(test)]
mod tests {
    use super::*;
    use nom::IResult;

    #[test]
    fn test_parse_variable_code() {
        let input = "B20005E_045M";
        let expected = VariableCode {
            table_code: TableCode {
                prefix: TablePrefix::B,
                table_id: "20005".to_owned(),
                suffix: Some("E".to_owned()),
            },
            column_id: "045".to_owned(),
            var_type: VariableType::MarginOfError,
        };
        assert_eq!(
            parse_variable_code(input.as_bytes()),
            IResult::Done(&b""[..], expected)
        );
    }

    #[test]
    fn test_parse_table_record() {
        let input = "B24126.  Detailed Occupation for the Full-Time, Year-Round Civilian Employed Female Population 16 Years and Over";
        let expected = TableRecord {
            code: TableCode {
                prefix: TablePrefix::B,
                table_id: "24126".to_owned(),
                suffix: None,
            },
            label: "Detailed Occupation for the Full-Time, Year-Round Civilian Employed Female Population 16 Years and Over".to_owned(),
        };

        assert_eq!(
            parse_table_record(input.as_bytes()),
            IResult::Done(&b""[..], expected)
        );
    }
}

