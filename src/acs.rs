use error::*;
use nom::{alpha, digit, rest, space, IResult};
use rusqlite;
use rusqlite::types::{FromSql, FromSqlError,FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use std::cmp::Ordering;
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
    pub variable: Variable,
    pub estimate: Estimate,
    pub year: u32, // I just use one big table, denormalized
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TablePrefix {
    B,
    C,
}

impl ToSql for TablePrefix {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl FromSql for TablePrefix {
    fn column_result(value: ValueRef) -> FromSqlResult<TablePrefix> {
        value.as_str().and_then(|val| {
            match val {
                "B" => Ok(TablePrefix::B),
                "C" => Ok(TablePrefix::C),
                _ => Err(FromSqlError::InvalidType),
            }
        })
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum VariableType {
    MarginOfError,
    Value,
}

impl ToSql for VariableType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}

impl FromSql for VariableType {
    fn column_result(value: ValueRef) -> FromSqlResult<VariableType> {
        value.as_str().and_then(|val| {
            match val {
                "M" => Ok(VariableType::MarginOfError),
                "E" => Ok(VariableType::Value),
                _ => Err(FromSqlError::InvalidType),
            }
        })
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

impl FromSql for Estimate {
    fn column_result(value: ValueRef) -> FromSqlResult<Estimate> {
        value.as_str().and_then(|val| {
            match val {
                "ACS 1-year estimate" => Ok(Estimate::OneYear),
                "ACS 5-year estimate" => Ok(Estimate::FiveYear),
                _ => Err(FromSqlError::InvalidType),
            }
        })
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

pub fn format_table_records(records: Vec<TableRecord>) -> String {
    let mut records = records;
    // TODO move this to trait
    records.sort_by(|ref a, ref b| {
        if a.code.table_id != b.code.table_id {
            a.code.table_id.cmp(&b.code.table_id)
        } else if a.code.prefix != b.code.prefix {
            a.code.prefix.cmp(&b.code.prefix)
        } else if a.code.suffix.is_none() && b.code.suffix.is_none() {
            Ordering::Equal
        } else if a.code.suffix.is_none() && !b.code.suffix.is_none() {
            Ordering::Less
        } else if !a.code.suffix.is_none() && b.code.suffix.is_none() {
            Ordering::Greater
        } else if !a.code.suffix.is_none() && !b.code.suffix.is_none() {
            a.code.suffix.as_ref().unwrap().cmp(&b.code.suffix.as_ref().unwrap())
        } else {
            Ordering::Equal
        }
    });

    let mut res = "code      | label\n==========|====================\n".to_owned();

    for record in records {
        let mut code = vec![
            record.code.prefix.to_string(),
            record.code.table_id,
        ];
        if let Some(suffix) = record.code.suffix {
            code.push(suffix);
        }
        let code = code.concat();

        res.push_str(&format!("{:10}| {}\n", code, record.label)[..]);
    }
    res
}

pub fn format_describe_table(records: Vec<VariableRecord>) -> String {
    let mut records = records;

    records.sort_by(|ref a, ref b| {
        let ref a = a.variable.code;
        let ref b = b.variable.code;

        if a.table_code.table_id != b.table_code.table_id {
            a.table_code.table_id.cmp(&b.table_code.table_id)
        } else if a.table_code.prefix != b.table_code.prefix {
            a.table_code.prefix.cmp(&b.table_code.prefix)
        } else if a.table_code.suffix.is_none() && b.table_code.suffix.is_none() {
            if a.column_id != b.column_id {
                a.column_id.cmp(&b.column_id)
            } else if a.var_type != b.var_type {
                a.var_type.cmp(&b.var_type)
            } else {
                    Ordering::Equal
            }
        } else if a.table_code.suffix.is_none() && !b.table_code.suffix.is_none() {
            Ordering::Less
        } else if !a.table_code.suffix.is_none() && b.table_code.suffix.is_none() {
            Ordering::Greater
        } else if !a.table_code.suffix.is_none() && !b.table_code.suffix.is_none() {
            if a.table_code.suffix.as_ref().unwrap() != b.table_code.suffix.as_ref().unwrap() {
                a.table_code.suffix.as_ref().unwrap().cmp(&b.table_code.suffix.as_ref().unwrap())
            } else {
                if a.column_id != b.column_id {
                    a.column_id.cmp(&b.column_id)
                } else if a.var_type != b.var_type {
                    a.var_type.cmp(&b.var_type)
                } else {
                    Ordering::Equal
                }
            }
        } else {
            Ordering::Equal
        }
    });
    let mut res = "
        code      | label\n\
        ==========|====================\n\

    ".to_owned();

    for record in records {
        let mut code = vec![
            record.variable.code.table_code.prefix.to_string(),
            record.variable.code.table_code.table_id,
        ];
        if let Some(suffix) = record.variable.code.table_code.suffix {
            code.push(suffix);
        }
        code.push("_".to_owned());
        code.push(record.variable.code.column_id);
        code.push(record.variable.code.var_type.to_string());
        let code = code.concat();

        res.push_str(&format!("{:10}| {:5}| {:10}|{}\n",
            code,
            record.year,
            record.estimate.to_string(),
            record.variable.label,
        )[..]);
    }
    res
}

// TODO move all this processing into sql query
// or at least refactor with format_describe
pub fn format_etl_config(records: Vec<VariableRecord>) -> String {
    let mut records = records;

    records.sort_by(|ref a, ref b| {
        let ref a = a.variable.code;
        let ref b = b.variable.code;

        if a.table_code.table_id != b.table_code.table_id {
            a.table_code.table_id.cmp(&b.table_code.table_id)
        } else if a.table_code.prefix != b.table_code.prefix {
            a.table_code.prefix.cmp(&b.table_code.prefix)
        } else if a.table_code.suffix.is_none() && b.table_code.suffix.is_none() {
            if a.column_id != b.column_id {
                a.column_id.cmp(&b.column_id)
            } else if a.var_type != b.var_type {
                a.var_type.cmp(&b.var_type)
            } else {
                    Ordering::Equal
            }
        } else if a.table_code.suffix.is_none() && !b.table_code.suffix.is_none() {
            Ordering::Less
        } else if !a.table_code.suffix.is_none() && b.table_code.suffix.is_none() {
            Ordering::Greater
        } else if !a.table_code.suffix.is_none() && !b.table_code.suffix.is_none() {
            if a.table_code.suffix.as_ref().unwrap() != b.table_code.suffix.as_ref().unwrap() {
                a.table_code.suffix.as_ref().unwrap().cmp(&b.table_code.suffix.as_ref().unwrap())
            } else {
                if a.column_id != b.column_id {
                    a.column_id.cmp(&b.column_id)
                } else if a.var_type != b.var_type {
                    a.var_type.cmp(&b.var_type)
                } else {
                    Ordering::Equal
                }
            }
        } else {
            Ordering::Equal
        }
    });

    let records = records.into_iter().filter(|record| {
        let last = record.variable.label.len();

        &record.variable.label.as_bytes()[last-1..] != &b":"[..] &&
        record.variable.code.var_type == VariableType::Value &&
        record.estimate == Estimate::FiveYear
    });
    let mut res = String::new();

    for record in records {
        let mut code = Vec::new();
        code.push(record.variable.code.column_id);
        code.push(record.variable.code.var_type.to_string());
        let code = code.concat();

        let label = record.variable.label.replace(":!!", "_").replace("'", "");
        let label = to_camelcase(&label);

        res.push_str(&format!("{}: {:?}\n",
            code,
            label,
        )[..]);
    }
    res
}

fn to_camelcase(s: &str) -> String {
    s.split_whitespace().map(|word| {
        let mut c = word.chars();

        match c.next() {
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            None => String::new(),
        }
    }).collect()
}

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

