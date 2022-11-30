use std::{fs::File, io::Read};

use crate::types::InputRow;

pub fn read_all(file_name: &str) -> Vec<InputRow> {
    let mut file = File::open(file_name).unwrap();

    let mut rows: Vec<InputRow> = Vec::new();

    let mut buf: [u8; 4] = [0; 4];

    loop {
        // Read ID - the first 32 bits of row
        if let Err(_) = file.read_exact(&mut buf){
            break;
        }
        let id = u32::from_ne_bytes(buf);
        // Read VALUE - the second 32 bits of row
        if let Err(_) = file.read_exact(&mut buf){
            break;
        }
        let val = f32::from_ne_bytes(buf);

        rows.push(InputRow(id, val));
    }

    rows
}