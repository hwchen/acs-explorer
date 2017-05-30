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
        .subcommand(SubCommand::with_name("find")
            .about("search for info on an acs table")
            .subcommand(SubCommand::with_name("table")
                .alias("t")
                .arg(Arg::with_name("find_table")
                    .takes_value(true)
                    .help("enter table id to search for")))
            .subcommand(SubCommand::with_name("label")
                .alias("l")
                .arg(Arg::with_name("find_label")
                    .takes_value(true)
                    .help("label to search for"))))
        .subcommand(SubCommand::with_name("describe")
            .alias("d")
            .arg(Arg::with_name("describe_table")
                .takes_value(true)
                .help("enter table id to describe")))
        .subcommand(SubCommand::with_name("refresh")
            .about("refresh all years and estimates of acs data summaries"))
        .after_help("Table ID search (find table subcommand):\n\
            \t- must start with a valid prefix (or no prefix for search).\n\
            \t- followed by required numerical table id.\n\
            \t- with optional table suffix. ")
        .get_matches();

    // for global flags. Check at each level/subcommand if the flag is present,
    // then flip switch.
    let mut verbose = false;
    if app_m.is_present("verbose") { verbose = true; }

    // Now section on matching subcommands and flags
    match app_m.subcommand() {
        ("find", Some(sub_m)) => {
            match sub_m.subcommand() {
                ("table", Some(sub_m)) => {
                    if sub_m.is_present("verbose") { verbose = true; }

                    let query = sub_m
                        .value_of("find_table")
                        .ok_or("Table id required for query")?;

                    let query = parse_table_query(query.as_bytes())
                        .to_result()
                        .map_err(|_| format!(
                            "{:?} is not a valid Table ID format, see --help",
                            query)
                        )?;

                    Ok(ExplorerCommand {
                        command: Command::FindTable(
                            FindTableQuery::ByTableId( query )
                        ),
                        verbose: verbose,
                        options: None,
                    })
                },
                ("label", Some(sub_m)) => {
                    if sub_m.is_present("verbose") { verbose = true; }

                    let query = sub_m
                        .value_of("find_label")
                        .ok_or("Text required to search by label")?;

                    Ok(ExplorerCommand {
                        command: Command::FindTable(
                            FindTableQuery::ByLabel( query.to_owned() )
                        ),
                        verbose:verbose,
                        options: None,
                    })
                },
                _ => Err("Not a valid subcommand".into()),
            }
        },
        ("describe", Some(sub_m)) => {
            if sub_m.is_present("verbose") { verbose = true; }

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
                    command: Command::DescribeTable(query),
                    verbose: verbose,
                    options: None,
                })
        },
        ("refresh", Some(sub_m)) => {
            if sub_m.is_present("verbose") { verbose = true; }

            Ok(ExplorerCommand {
                command: Command::Refresh,
                verbose: verbose,
                options: None,
            })
        },
        _ => Err("Not a valid subcommand".into()),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplorerCommand {
    pub command: Command,
    pub verbose: bool,
    pub options: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Refresh,
    FindTable(FindTableQuery),
    DescribeTable(TableIdQuery),
    FetchTable, // all, by year, acs estimate
}

#[derive(Debug, Clone, PartialEq)]
pub enum FindTableQuery {
    ByTableId(TableIdQuery),
    ByLabel(String),
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
        prefix: alt!(tag!("B") | tag!("C")) >>

        (match prefix {
            b"B" => TablePrefix::B,
            b"C" => TablePrefix::C,
            _ => TablePrefix::B, // TODO Fix error handling later
        })
    ))
);
