use payments::output::{dump_result, make_client_output_records};
use payments::process_csv;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<String>>();

    if args.len() != 2 {
        eprintln!("Usage: {} <input csv file>", args[0]);
        std::process::exit(1);
    }

    let input_file = &args[1];

    let processed = process_csv(&input_file)?;
    let output = make_client_output_records(&processed);
    dump_result(output)?;

    Ok(())
}
