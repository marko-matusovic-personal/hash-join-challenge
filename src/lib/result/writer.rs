use std::{fs::File, io::Write};

use crate::types::JoinRow;

pub struct JoinWriter {
    file: File,
}

impl JoinWriter {
    pub fn create(file_name: &str) -> JoinWriter {
        JoinWriter {
            file: File::create(file_name).expect("Unable to create file!"),
        }
    }

    pub fn write(&mut self, JoinRow(id, val_1, val_2): &JoinRow){
        self.file.write(&id.to_ne_bytes()).unwrap();
        self.file.write(&val_1.to_ne_bytes()).unwrap();
        self.file.write(&val_2.to_ne_bytes()).unwrap();
    }

    pub fn close(&mut self) {
        self.file.flush().unwrap();
    }
}
