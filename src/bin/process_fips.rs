use std::io::Read;
use std::fs::File;

fn main() {
    let mut f = File::open("state.txt").unwrap();
    let mut buf = String::new();

    f.read_to_string(&mut buf).unwrap();

    let data: Vec<Vec<_>> = buf.lines().map(|line| line.split('|').collect()).collect();

    // Just enums
    for line in data.iter() {
        println!("{},", line[2]);
    }
    // match to fips
    for line in data.iter() {
        println!("{} => {:?},", line[2], line[0]);
    }

    // match to post
    for line in data.iter() {
        println!("{} => {:?},", line[2], line[1]);
    }

    // match to giniid
    for line in data.iter() {
        println!("{} => {:?},", line[2], line[3]);
    }
}
