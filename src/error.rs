use fst;
use reqwest;
use rusqlite;

error_chain! {
    foreign_links {
        Io(::std::io::Error);
        Reqwest(reqwest::Error);
        ReqwestUrl(reqwest::UrlError);
        Rusqlite(rusqlite::Error);
        Fst(fst::Error);
    }
}
