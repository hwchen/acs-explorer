use error::*;

use fst::{IntoStreamer, Streamer, Map, MapBuilder};
use std::fs::File;
use std::io;
use std::path::Path;

//TODO should searchbuilder be generic, instead of bufwriter?
pub struct SearchBuilder<W: io::Write> {
    map_builder: MapBuilder<io::BufWriter<W>>,
}

impl<W: io::Write> SearchBuilder<W> {
    pub fn new(writer: W) -> Result<Self> {
        let writer = io::BufWriter::new(writer);

        Ok(SearchBuilder {
            map_builder: MapBuilder::new(writer)?,
        })
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, val: u64) -> Result<()> {
        self.map_builder.insert(key, val)
            .chain_err(|| "Error inserting into search index")
    }

    pub fn finish(self) -> Result<()> {
        self.map_builder.finish()
            .chain_err(|| "Error finalizing searhc index build")
    }
}

pub struct Search {
    map: Map,
}

impl Search {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Search {
            map: Map::from_path(path)?,
        })
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<u64> {
        self.map.get(key)
    }
}

