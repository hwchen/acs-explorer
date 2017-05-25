use clap::{Arg, App, AppSettings, SubCommand};

use acs::{
    TablePrefix,
    TableCode,
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
        .subcommand(SubCommand::with_name("table")
            .about("search for info on an acs table")
            .arg(Arg::with_name("table_query")
                 .takes_value(true)
                 .help("table id to search for")))
        .subcommand(SubCommand::with_name("refresh")
            .about("refresh all years and estimates of acs data summaries"))
        .after_help("Table ID search (table subcommand):\n\
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
        ("table", Some(sub_m)) => {
            let table_id = sub_m
                .value_of("table_query")
                .ok_or("Table ID required for query")?;
            let table_query = parse_table_query(table_id.as_bytes())
                .to_result()
                .map_err(|_| format!(
                    "{:?} is not a valid Table ID format, see --help", table_id))?;
            if sub_m.is_present("verbose") { verbose = true; }

            Ok(ExplorerCommand {
                command: table_query,
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
    verbose: bool,
    options: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Refresh,
    TableQuery {
        prefix: Option<TablePrefix>,
        table_id: String,
        suffix: Option<String>,
    },
    VariableQuery,
}

named!(parse_table_query<&[u8], Command>,
    do_parse!(
        prefix: parse_prefix_query >>
        table_id: parse_table_id >>
        suffix: map!(opt!(complete!(parse_suffix)), |s| {
            match s {
                None => None,
                Some(s) => s,
            }
        })>>

        (Command::TableQuery {
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
