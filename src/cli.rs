use clap::{Arg, App, AppSettings, SubCommand};

use acs::{
    TablePrefix,
    parse_table_id,
    parse_suffix,
};
use error::*;

pub fn cli_command() -> Result<ExplorerCommand> {
    let app_m = App::new("ACS Explorer")
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(Arg::with_name("verbose")
             .short("v")
             .global(true))
        .subcommand(SubCommand::with_name("search")
            .about("fulltext search for an acs table")
            .alias("s")
            .arg(Arg::with_name("search_tables")
                .takes_value(true)
                .help("enter text to search for")))
        .subcommand(SubCommand::with_name("describe")
            .about("Get information about a specific table")
            .alias("d")
            .arg(Arg::with_name("describe_table")
                .takes_value(true)
                .help("enter table id to describe"))
            .arg(Arg::with_name("etl_config")
                .short("e")
                .long("etl")
                .conflicts_with("raw")
                .help("format results to etl config"))
            .arg(Arg::with_name("raw")
                .short("r")
                .long("raw")
                .help("format results as raw data from api")))
        .subcommand(SubCommand::with_name("refresh")
            .about("refresh all years and estimates of acs data summaries"))
        .after_help("fulltext search (`search` table subcommand):\n\
            \t- Currently exact match.\n
            \t- Case insensitive.\n\
            \t- Searches table name, and table id (no prefix or suffix). ")
        .get_matches();

    // for global flags. Check at each level/subcommand if the flag is present,
    // then flip switch.
    let mut verbose = app_m.is_present("verbose");

    // Now section on matching subcommands and flags
    match app_m.subcommand() {
        ("search", Some(sub_m)) => {
            if sub_m.is_present("verbose") { verbose = true; }

            let search = sub_m
                .value_of("search_tables")
                .ok_or("No text entered")?;

            Ok(ExplorerCommand {
                command: Command::FulltextSearch(search.to_owned()),
                verbose: verbose,
            })
        },
        ("describe", Some(sub_m)) => {
            if sub_m.is_present("verbose") { verbose = true; }

            let etl_config = sub_m.is_present("etl_config");
            let raw = sub_m.is_present("raw");

            let query = sub_m
                .value_of("describe_table")
                .ok_or("Table id required for query")?;

            let query = parse_table_query(query.as_bytes())
                .to_result()
                .map_err(|_| format!(
                    "{:?} is not a valid Table ID format, see --help",
                    query)
                )?;

            if query.prefix.is_none() {
                return Err("Prefix required for table code".into());
            }

            Ok(ExplorerCommand {
                command: Command::DescribeTable {
                    query: query,
                    etl_config: etl_config,
                    raw: raw,
                },
                verbose: verbose,
            })
        },
        ("refresh", Some(sub_m)) => {
            if sub_m.is_present("verbose") { verbose = true; }

            Ok(ExplorerCommand {
                command: Command::Refresh,
                verbose: verbose,
            })
        },
        _ => Err("Not a valid subcommand".into()),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplorerCommand {
    pub command: Command,
    pub verbose: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Refresh,
    FulltextSearch(String),
    DescribeTable {
        query: TableIdQuery,
        etl_config: bool,
        raw: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableIdQuery {
    pub prefix: Option<TablePrefix>,
    pub table_id: String,
    pub suffix: Option<String>,
}

named!(parse_table_query<&[u8], TableIdQuery>,
    do_parse!(
        prefix: parse_prefix_query >>
        table_id: parse_table_id >>
        suffix: map!(opt!(complete!(parse_suffix)), |s| {
            match s {
                None => None,
                Some(s) => s,
            }
        })>>

        (TableIdQuery {
                prefix: prefix,
                table_id: table_id,
                suffix: suffix,
        })
    )
);

named!(parse_prefix_query<&[u8], Option<TablePrefix> >,
    opt!(do_parse!(
        prefix: alt!(tag!("B") | tag!("b") | tag!("C") | tag!("c")) >>

        (match prefix {
            b"B" | b"b" => TablePrefix::B,
            b"C" | b"c" => TablePrefix::C,
            _ => TablePrefix::B, // TODO Fix error handling later
        })
    ))
);
