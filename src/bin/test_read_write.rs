use file_handling::{self, input::{write::write_all, read::read_all}, types::InputRow};

fn main(){
    let v: Vec<InputRow> = vec![InputRow(1, 1.5), InputRow(2, 2.5), InputRow(3, 3.5)];
    let file = "data/test.u32f32";
    write_all(file, &v);
    let u = read_all(file);
    println!("{:?} -> {:?}", v, u);
}