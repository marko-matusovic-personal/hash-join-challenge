use std::{fs, io::Write};

use crate::types::InputRow;

pub fn write_all(file_name: &str, rows: &Vec<InputRow>) {
    // let file = match fs::File::create(file_name) {
    //     Ok(file) => file,
    //     Err(error) => panic!("Could not create file! {:?}", error)
    // };
    let mut file = fs::File::create(file_name).unwrap();
    for &InputRow(id, val) in rows {
        file.write(&id.to_ne_bytes()).unwrap();
        file.write(&val.to_ne_bytes()).unwrap();
    }
    file.flush().unwrap();
}