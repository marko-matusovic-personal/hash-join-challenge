use std::{collections::HashMap, time::Instant};

use file_handling::{
    input::reader::Reader,
    result::writer::JoinWriter,
    types::{InputRow, JoinRow},
};
use rand::Rng;

const VARIANT: &str = "500k";

pub fn run() {
    let clock = Instant::now();
    run_simple_hash_join();
    println!("Simple hash join took: {}ms", clock.elapsed().as_millis());

    let clock = Instant::now();
    run_classic_hash_join();
    println!("Classic hash join took: {}ms", clock.elapsed().as_millis());
}

fn run_simple_hash_join() {
    let path_r: &str = &format!("data/R-{}.u32f32", VARIANT);
    let path_s: &str = &format!("data/S-{}.u32f32", VARIANT);

    let mut reader_r = Reader::open(&path_r);
    let mut reader_s = Reader::open(&path_s);

    let mut map: HashMap<String, Vec<InputRow>> = HashMap::new();

    let mut r_size: u32 = 0;
    let mut s_size: u32 = 0;

    loop {
        if let Some(InputRow(id, val)) = reader_r.read() {
            r_size += 1;
            let hsh = blake3::hash(&id.to_ne_bytes()).to_string();
            map.entry(hsh).or_insert(Vec::new()).push(InputRow(id, val));
            continue;
        }
        break;
    }

    let mut join: Vec<JoinRow> = Vec::new();

    loop {
        if let Some(InputRow(id_s, val_s)) = reader_s.read() {
            s_size += 1;
            let hsh = blake3::hash(&id_s.to_ne_bytes()).to_string();
            if let Some(rows) = map.get(&hsh) {
                rows.iter()
                    .for_each(|&InputRow(_, val_r)| join.push(JoinRow(id_s, val_s, val_r)));
            }
            continue;
        }
        break;
    }

    println!("Size of R: {} entries", r_size);
    println!("Size of S: {} entries", s_size);
    println!("Size of R |x| S: {} entries", join.len());
}

fn run_classic_hash_join() {
    let path_r: &str = &format!("data/R-{}.u32f32", VARIANT);
    let path_s: &str = &format!("data/S-{}.u32f32", VARIANT);
    let path_out: &str = &format!("data/R|x|S-{}.u32f32", VARIANT);

    println!("Started classic hash join");

    let mut reader_r = Reader::open(&path_r);
    let mut reader_s = Reader::open(&path_s);

    let mut map: HashMap<String, Vec<InputRow>> = HashMap::new();

    let mut writer = JoinWriter::create(path_out);

    let mut block = 0;
    let mut row_count = 0;

    'out: loop {
        println!("Hashing block {}", block);
        block += 1;

        map.clear();

        let mut block_size = 0;
        loop {
            if running_low_on_memory() {
                // out of memory check
                println!("Block size {} entries", block_size);
                break;
            }
            if let Some(InputRow(id, val)) = reader_r.read() {
                block_size += 1;
                let hsh = blake3::hash(&id.to_ne_bytes()).to_string();
                map.entry(hsh).or_insert(Vec::new()).push(InputRow(id, val));
                continue;
            }
            break 'out;
        }

        loop {
            if let Some(InputRow(id_s, val_s)) = reader_s.read() {
                let hsh = blake3::hash(&id_s.to_ne_bytes()).to_string();
                if let Some(rows) = map.get(&hsh) {
                    rows.iter().for_each(|&InputRow(_, val_r)| {
                        row_count += 1;
                        writer.write(&JoinRow(id_s, val_s, val_r));
                    });
                }
                continue;
            }
            break;
        }
    }
    println!("Size of R |x| S: {} entries", row_count);

    println!("Finished");
}

fn running_low_on_memory() -> bool {
    let mut rng = rand::thread_rng();
    return rng.gen_ratio(1, 100_000);
}
