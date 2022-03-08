pub mod input;
pub mod output;

use csv::StringRecord;
use input::{make_input_record, InputRecord};

pub fn process_csv(fname: &str) -> Result<Vec<InputRecord>, Box<dyn std::error::Error>> {
    let mut res: Vec<InputRecord> = Vec::new();
    let mut reader = csv::Reader::from_path(fname)?;
    for result in reader.records() {
        let record = result?;
        let pos = record.position().expect("Couldn't determine position");
        let mut s_record = StringRecord::from(record.clone());
        s_record.trim();
        match make_input_record(&s_record) {
            Some(r) => res.push(r),
            None => eprintln!("Invalid record on line {}", pos.line()),
        }
    }
    Ok(res)
}
