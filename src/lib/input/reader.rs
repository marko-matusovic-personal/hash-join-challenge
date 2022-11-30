use std::{fs::File, io::Read};

use crate::types::InputRow;

pub struct Reader {
    file_name: String,
    file: File,
}

impl Reader {
    pub fn open(file_name: &str) -> Reader {
        Reader {
            file_name: file_name.to_owned(),
            file: File::open(file_name).expect("Unable to open file!"),
        }
    }

    pub fn read(&mut self) -> Option<InputRow> {
        let mut buf: [u8; 4] = [0; 4];

        // Read ID - the first 32 bits of row
        if let Err(_) = self.file.read_exact(&mut buf){
            return None;
        }
        let id = u32::from_ne_bytes(buf);
        // Read VALUE - the second 32 bits of row
        if let Err(_) = self.file.read_exact(&mut buf){
            panic!("File alignment error during reading the file: {}", self.file_name);
        }
        let val = f32::from_ne_bytes(buf);

        Some(InputRow(id, val))
    }
}