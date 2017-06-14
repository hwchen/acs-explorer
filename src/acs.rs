use error::*;
use nom::{alpha, digit, rest, space, IResult};
use rusqlite;
use rusqlite::types::{FromSql, FromSqlError,FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use std::cmp::Ordering;
use std::collections::HashMap;
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
                .map(|s| s.to_owned().to_uppercase())
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariableRecord {
    pub label: String, // Encodes Hierarchy
    pub code: VariableCode,
    pub year: u32,
    pub estimate: Estimate,
}

impl Ord for VariableRecord {
    fn cmp(&self, other: &VariableRecord) -> Ordering {
        self.code.cmp(&other.code)
    }
}

impl PartialOrd for VariableRecord {
    fn partial_cmp(&self, other: &VariableRecord) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VariableCode {
    pub table_code: TableCode,
    pub column_id: String,
    pub var_type: VariableType,
}

impl Ord for VariableCode {
    fn cmp(&self, other: &VariableCode) -> Ordering {
        if self.table_code != other.table_code {
            self.table_code.cmp(&self.table_code)
        } else if self.column_id != other.column_id {
            self.column_id.cmp(&other.column_id)
        } else if self.var_type != other.var_type {
            self.var_type.cmp(&other.var_type)
        } else {
                Ordering::Equal
        }
    }
}

impl PartialOrd for VariableCode {
    fn partial_cmp(&self, other: &VariableCode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRecord {
    pub code: TableCode,
    pub label: String,
}

impl Ord for TableRecord {
    fn cmp(&self, other: &TableRecord) -> Ordering {
        self.code.cmp(&other.code)
    }
}

impl PartialOrd for TableRecord {
    fn partial_cmp(&self, other: &TableRecord) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableCode {
    pub prefix: TablePrefix,
    pub table_id: String,
    pub suffix: Option<String>, // should be limited to upper-case letters?
}

impl Ord for TableCode {
    fn cmp(&self, other: &TableCode) -> Ordering {
        if self.table_id != other.table_id {
            self.table_id.cmp(&other.table_id)
        } else if self.prefix != other.prefix {
            self.prefix.cmp(&other.prefix)
        } else if self.suffix.is_none() && other.suffix.is_none() {
            Ordering::Equal
        } else if self.suffix.is_none() && !other.suffix.is_none() {
            Ordering::Less
        } else if !self.suffix.is_none() && other.suffix.is_none() {
            Ordering::Greater
        } else if !self.suffix.is_none() && !other.suffix.is_none() {
            self.suffix.as_ref().unwrap().cmp(&other.suffix.as_ref().unwrap())
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for TableCode {
    fn partial_cmp(&self, other: &TableCode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
        match *self {
            Estimate::OneYear => Ok(ToSqlOutput::from("1yr")),
            Estimate::FiveYear => Ok(ToSqlOutput::from("5yr")),
        }
    }
}

impl FromSql for Estimate {
    fn column_result(value: ValueRef) -> FromSqlResult<Estimate> {
        value.as_str().and_then(|val| {
            match val {
                "1yr" => Ok(Estimate::OneYear),
                "5yr" => Ok(Estimate::FiveYear),
                _ => Err(FromSqlError::InvalidType),
            }
        })
    }
}

pub fn format_table_records(records: Vec<TableRecord>) -> String {
    let mut records = records;
    records.sort();

    let mut res = "code      | label\n==========|====================\n".to_owned();

    for record in records {
        res.push_str(&format_table_name(&record));
        res.push_str("\n");
    }
    res
}

pub fn format_table_name(record: &TableRecord) -> String {
    let mut code = record.code.prefix.to_string();
    code.push_str(&record.code.table_id);
    if let Some(ref suffix) = record.code.suffix {
        code.push_str(suffix);
    }
    format!("{} | {}\n", code, record.label)
}

struct TableVersion {
    records: Vec<VariableRecord>,
    min_year: u32,
    max_year: u32,
}

// TODO
// This is a quick runtime hack to check for records.
// I should go back and fix the table structure so that
// calculation is done at refresh and runtime is faster
fn get_table_versions(all_versions: Vec<VariableRecord>) -> Vec<TableVersion> {
    let mut all_versions = all_versions;
    all_versions.sort();

    let mut versions: Vec<TableVersion> = Vec::new();

    // check a year, and est combo
    for year in 2009..2016 {
        for estimate in &[Estimate::FiveYear, Estimate::OneYear] {
            let current_records: Vec<_> = all_versions.iter().filter(|record| {
                record.year == year &&
                record.estimate == *estimate
            })
            .cloned()
            .collect();

            let mut is_new_version = false;

            if let Some(last_version) = versions.last() {
                // First check if tables are different length (with current being
                // non-empty, so that it's useful to check)
                // then must be different versions without checking further
                if current_records.len() != 0 && current_records.len() != last_version.records.len() {
                    is_new_version = true;
                } else if current_records.len() != 0 {
                    // if table lengths are same, then check further, if any labels
                    // different, if current version is not empty vec
                    let are_different_versions = last_version.records.iter()
                        .zip(current_records.iter())
                        .any(|(last, current)| {
                            last.label.to_lowercase() != current.label.to_lowercase()
                        });

                    if are_different_versions {
                        is_new_version = true;
                    }
                }
            } else {
                // If there is no last_version, it means versions
                // is empty, so fill with the current one.
                is_new_version = true;
            }

            if is_new_version {
                let min_year = current_records[0].year;

                if let Some(last_version) = versions.last_mut() {
                    last_version.max_year = min_year - 1;
                }
                versions.push(TableVersion {
                    records: current_records,
                    min_year: min_year,
                    max_year: 2016,
                });
            }
        }
    }
    versions
}

pub fn format_describe_table_raw(year: u32, records: Vec<VariableRecord>) -> String {
    let versions = get_table_versions(records);

    let mut res = String::new();

    for table_version in versions {
        //res.push_str(&format!("First year used: {}\n", table_version[0].year));
        for record in table_version.records {
            let mut code = vec![
                record.code.table_code.prefix.to_string(),
                record.code.table_code.table_id,
            ];
            if let Some(suffix) = record.code.table_code.suffix {
                code.push(suffix);
            }
            code.push("_".to_owned());
            code.push(record.code.column_id);
            code.push(record.code.var_type.to_string());
            let code = code.concat();

            res.push_str(&format!("{} {}\n",
                code,
                record.label,
            )[..]);
        }
        res.push_str("\n");
    }
    res
}

pub fn format_describe_table_pretty(year: u32, records: Vec<VariableRecord>) -> String {
    let versions = get_table_versions(records);

    let indent = "    ";

    let mut res = "\n\
        code | label\n\
        =====|====================================\n\

    ".to_owned();

    for table_version in versions {
        res.push_str(&format!("Years: {}-{}\n", table_version.min_year, table_version.max_year));

        let table_records = table_version.records.into_iter().filter(|ref record| {
            record.code.var_type == VariableType::Value
        });


        for record in table_records {
            let col_id = record.code.column_id;

            // format label by only showing the last part of label,
            // with appropriate indentation

            // find index to slice at
            let split_index = match record.label.rfind(":!!") {
                Some(i) => i + 3,
                None => 0,
            };

            let (indents, label) = record.label.split_at(split_index);

            // There's an extra :!! at the end of indents. So
            // skip1
            let indents: String = indents.split(":!!").skip(1)
                .map(|_| indent)
                .collect();

            let label = label.trim_right_matches(":");


            res.push_str(&format!("{:5}| {}{}\n",
                col_id,
                indents,
                label,
            )[..]);
        }
    }
    res

}

// TODO move all this processing into sql query
// or at least refactor with format_describe
pub fn format_etl_config(year: u32, records: Vec<VariableRecord>) -> String {
    let versions = get_table_versions(records);

    let indents = "    ";

    let mut res = String::new();

    for mut table_version in versions {
        let mut records = table_version.records;
        // Figure out better way to trim?
        if records.len() == 2 {
            records[1].label = records[1].label.trim_right_matches(":").to_owned();
        }

        let table_code = records[0].code.table_code.prefix.to_string() +
            &records[0].code.table_code.table_id;

        let min_year = table_version.min_year;
        let max_year = table_version.max_year;

        let records = records.into_iter().filter(|record| {
            let last = record.label.len();

            &record.label.as_bytes()[last-1..] != &b":"[..] &&
            record.code.var_type == VariableType::Value
        });


        res.push_str(&format!("\
            name: {:?}\n\
            tag: \"acs\"\n\
            min_year: {}\n\
            max_year: {}\n\
            acs_table:\n\
                {}id: {:?}\n\
                {}value_label: {:?}\n\
                {}dimension_labels: [\n\
                {}{:?},\n\
                {}]\n\
                {}columns:\n\
        ",
            "TABLENAME",
            min_year,
            max_year,
            indents, table_code,
            indents, "population", // logic to make this dynamic
            indents,
            indents.repeat(2), "DIMENSION",
            indents,
            indents,
        ));

        for record in records {
            let mut code = Vec::new();
            code.push(record.code.column_id);
            code.push(record.code.var_type.to_string());
            let code = code.concat();

            let label = record.label.replace(":!!", "_").replace("'", "");
            let label = to_camelcase(&label);

            let indents = indents.repeat(2);
            res.push_str(&format!("{}{}: {:?}\n",
                indents,
                code,
                label,
            )[..]);
        }
        res.push_str("\n");
    }
    res
}

pub fn format_est_years(est_years: &HashMap<Estimate, Vec<u32>>) -> String {
    let mut res = String::new();
    for (estimate, years) in est_years.iter() {
        res.push_str(&format!("{}: {:?}\n", estimate, years)[..]);
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

