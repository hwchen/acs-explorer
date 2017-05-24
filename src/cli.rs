use clap::{Arg, App, AppSettings, SubCommand};
use error::*;

pub fn cli_config() -> Result<ExplorerConfig> {
    let matches = App::new("ACS Explorer")
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(SubCommand::with_name("tables")
            .about("search for info on acs tables")
            .arg(Arg::with_name("table_query")
                 .takes_value(true)
                 .help("table id to search for")))
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("tables") {
        let table_id = matches.value_of("table_query").ok_or("table id required for query")?;
        println!("{}", table_id);
    }
    Ok(ExplorerConfig)
}

pub struct ExplorerConfig;
