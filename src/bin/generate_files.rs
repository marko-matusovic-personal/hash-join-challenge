use std::{collections::HashSet, time::Instant};

use rand::{self, seq::SliceRandom, Rng};

use file_handling::{self, types::InputRow, input::write::write_all};

const VARIANT: &str = "500M";
const COUNT: isize = 500_000_000;
const PATH: &str = "data/";

fn main() {
    let total = Instant::now();
    let clock = Instant::now();

    let mut rng = rand::thread_rng();

    let mut ids: HashSet<u32> = HashSet::new();

    for i in 0..COUNT {
        if i % 100_000 == 0 {
            print!("Generating ids {:.2}% done.\r", 100f64 * (i as f64) / (COUNT as f64));
        }
        ids.insert(loop {
            let n = rng.gen::<u32>();
            if !ids.contains(&n) {
                break n;
            }
        });
    }

    let mut ids: Vec<u32> = Vec::from_iter(ids);
    ids.shuffle(&mut rng);

    println!(
        "{:30}{:6} ms",
        "Generating unique ids took",
        clock.elapsed().as_millis()
    );
    let clock = Instant::now();

    let rows: Vec<InputRow> = ids
        .iter()
        .map(|&id| InputRow(id, rand::random::<f32>()))
        .collect();

    // panic!("size of rows: {} bits", rows.len() * (32 + 32));

    println!(
        "{:30}{:6} ms",
        "Generating values of R took",
        clock.elapsed().as_millis()
    );
    let clock = Instant::now();

    write_all(&format!("{}R-{}.u32f32", PATH, VARIANT), &rows);

    println!(
        "{:30}{:6} ms",
        "Writing R file took",
        clock.elapsed().as_millis()
    );
    let clock = Instant::now();

    ids.shuffle(&mut rng);
    let rows: Vec<InputRow> = ids
        .iter()
        .map(|&id| InputRow(id, rand::random::<f32>()))
        .collect();

    println!(
        "{:30}{:6} ms",
        "Generating values of S took",
        clock.elapsed().as_millis()
    );
    let clock = Instant::now();

    write_all(&format!("{}S-{}.u32f32", PATH, VARIANT), &rows);

    println!(
        "{:30}{:6} ms",
        "Writing S file took",
        clock.elapsed().as_millis()
    );

    println!("{}", ["-"; 39].join(""));

    println!("{:30}{:6} ms", "Total is", total.elapsed().as_millis());
}
