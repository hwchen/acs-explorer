use clap::{Arg, App, AppSettings, SubCommand};

use acs;
use error::*;

pub fn cli_command() -> Result<ExplorerCommand> {
    let app_m = App::new("ACS Explorer")
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(SubCommand::with_name("table")
            .about("search for info on an acs table")
            .arg(Arg::with_name("table_query")
                 .takes_value(true)
                 .help("table id to search for")))
        .subcommand(SubCommand::with_name("refresh")
            .about("refresh all years and estimates of acs data summaries"))
        .get_matches();

    match app_m.subcommand() {
        ("table", Some(sub_m)) => {
            let table_id = sub_m
                .value_of("table_query")
                .ok_or("Table ID required for query")?;
            Ok(ExplorerCommand {
                command: Command::TableQuery {
                    prefix: None,
                    table_id: table_id.to_owned(),
                    suffix: None,
                },
                options: None,
            })
        },
        ("refresh", _) => {
            Ok(ExplorerCommand {
                command: Command::Refresh,
                options: None,
            })
        },
        _ => Err("Not a valid subcommand".into()),
    }
}

pub struct ExplorerCommand {
    pub command: Command,
    options: Option<String>,
}

pub enum Command {
    Refresh,
    TableQuery {
        prefix: Option<acs::TablePrefix>,
        table_id: String,
        suffix: Option<String>,
    },
    VariableQuery,
}

//named!(parse_table_code<&[u8], TableCode>,
//    do_parse!(
//        prefix: parse_prefix >>
//        table_id: parse_table_id >>
//        suffix: parse_suffix >>
//
//        (TableCode {
//                prefix: prefix,
//                table_id: table_id,
//                suffix: suffix,
//        })
//    )
//);
//
//named!(parse_prefix<&[u8], TablePrefix>,
//    do_parse!(
//        prefix: alt!(tag!("B") | tag!("C")) >>
//
//        (match prefix {
//            b"B" => TablePrefix::B,
//            b"C" => TablePrefix::C,
//            _ => TablePrefix::B, // TODO Fix error handling later
//        })
//    )
//);
//
//named!(parse_table_id<&[u8], String>,
//    map_res!(
//        digit,
//        |id| str::from_utf8(id).map(|s| s.to_owned())
//    )
//);
//
//named!(parse_suffix<&[u8], Option<String> >,
//    opt!(map_res!(
//        alpha,
//        |suffix| {
//            str::from_utf8(suffix)
//                .map(|s| s.to_owned())
//        }
//    ))
//);
//
