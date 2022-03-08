use super::input::{InputRecord, TransactionType};
use serde::{ser::Serializer, Serialize};
use std::collections::HashMap;

/// An `OutputRecord` is used to store processed data from a
/// single client.
#[derive(Debug, Copy, Clone, Default, PartialEq, Serialize)]
pub struct OutputRecord {
    pub client: u16,
    #[serde(serialize_with = "round_to_4_dp")]
    pub available: f64,
    #[serde(serialize_with = "round_to_4_dp")]
    pub held: f64,
    #[serde(serialize_with = "round_to_4_dp")]
    pub total: f64,
    pub locked: bool,
}

impl OutputRecord {
    // Convenience function to quickly build a new `OutputRecord` struct.
    fn new(client: u16, available: f64, held: f64, total: f64, locked: bool) -> Self {
        OutputRecord {
            client,
            available,
            held,
            total,
            locked,
        }
    }
}

/// Instead of importing more crates, I decided to create a simple serializer
/// function for values of type `f64`. This function takes an f64 as input
/// and serializes it to an f64 rounded to 4 decimal places.
fn round_to_4_dp<S>(input: &f64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_f64(format!("{:.4}", input).parse::<f64>().unwrap())
}

fn get_transaction_amount(
    records: &Vec<InputRecord>,
    client: u16,
    transaction_id: u32,
) -> Option<f64> {
    for record in records {
        if record.client == client && record.tx == transaction_id {
            if let Some(amt) = record.amount {
                return Some(amt);
            };
        }
    }
    None
}

/// This function checks whether a dispute exists for the given client and
/// transaction ID. This is used when resolving a dispute.
fn check_dispute(records: &Vec<InputRecord>, client: u16, transaction_id: u32) -> bool {
    for record in records {
        if record.r#type == TransactionType::Dispute
            && record.client == client
            && record.tx == transaction_id
        {
            return true;
        }
    }
    false
}

/// This function takes as input a vector of type `InputRecord` and performs the calculations
/// necessary to compute the balance of each client. It uses a HashMap to store the given
/// client ID and current client balances. The HashMap is mutated in-place to minimize
/// the amount of memory manipulations.
pub fn make_client_output_records(input_records: &Vec<InputRecord>) -> Vec<OutputRecord> {
    let mut output: HashMap<u16, OutputRecord> = HashMap::new();
    for record in input_records {
        match record.r#type {
            TransactionType::Deposit => {
                if output.contains_key(&record.client) {
                    output.get_mut(&record.client).unwrap().available += record.amount.unwrap();
                    output.get_mut(&record.client).unwrap().total += record.amount.unwrap();
                } else {
                    let temp = OutputRecord::new(
                        record.client,
                        record.amount.unwrap(),
                        0.0,
                        record.amount.unwrap(),
                        false,
                    );

                    output.insert(record.client, temp);
                }
            }
            TransactionType::Withdrawal => {
                if output.contains_key(&record.client) {
                    if record.amount.unwrap() <= output.get(&record.client).unwrap().available {
                        output.get_mut(&record.client).unwrap().available -= record.amount.unwrap();
                        output.get_mut(&record.client).unwrap().total -= record.amount.unwrap();
                    }
                }
            }
            TransactionType::Dispute => {
                if output.contains_key(&record.client) {
                    if let Some(transaction) =
                        get_transaction_amount(&input_records, record.client, record.tx)
                    {
                        output.get_mut(&record.client).unwrap().available -= transaction;
                        output.get_mut(&record.client).unwrap().held += transaction;
                    }
                }
            }
            TransactionType::Resolve => {
                if output.contains_key(&record.client) {
                    if let Some(transaction) =
                        get_transaction_amount(&input_records, record.client, record.tx)
                    {
                        if check_dispute(&input_records, record.client, record.tx) {
                            output.get_mut(&record.client).unwrap().available += transaction;
                            output.get_mut(&record.client).unwrap().held -= transaction;
                        }
                    }
                }
            }
            TransactionType::Chargeback => {
                if output.contains_key(&record.client) {
                    if let Some(transaction) =
                        get_transaction_amount(&input_records, record.client, record.tx)
                    {
                        output.get_mut(&record.client).unwrap().total -= transaction;
                        output.get_mut(&record.client).unwrap().held -= transaction;
                        output.get_mut(&record.client).unwrap().locked = true;
                    }
                }
            }
        }
    }

    // Dump the values of each client's balance as
    // a vector.
    let res = output.values().cloned().collect();
    res
}

/// This function simply dumps a vector of type `OutputRecord` to standard out.
pub fn dump_result(values: Vec<OutputRecord>) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = csv::Writer::from_writer(std::io::stdout());
    for val in values {
        writer.serialize(val)?;
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::super::input::make_input_record;
    use super::{check_dispute, get_transaction_amount, OutputRecord};
    use csv::StringRecord;

    #[test]
    fn test_new_valid_output_record() {
        let test_record = OutputRecord {
            client: 1,
            available: 1.0,
            held: 0.0,
            total: 1.0,
            locked: false,
        };
        assert_eq!(OutputRecord::new(1, 1.0, 0.0, 1.0, false), test_record);
    }

    #[test]
    fn test_valid_client_transaction() {
        let record = vec![make_input_record(&StringRecord::from(vec![
            "deposit", "1", "1", "20.00",
        ]))
        .unwrap()];
        assert_eq!(get_transaction_amount(&record, 1, 1), Some(20.00));
    }

    #[test]
    fn test_valid_dispute() {
        let record =
            vec![make_input_record(&StringRecord::from(vec!["dispute", "1", "1", ""])).unwrap()];

        assert_eq!(check_dispute(&record, 1, 1), true);
    }

    #[test]
    fn test_invalid_dispute_client() {
        let record =
            vec![make_input_record(&StringRecord::from(vec!["dispute", "1", "1", ""])).unwrap()];

        assert_eq!(check_dispute(&record, 100, 1), false);
    }

    #[test]
    fn test_invalid_dispute_transaction() {
        let record =
            vec![make_input_record(&StringRecord::from(vec!["dispute", "1", "1", ""])).unwrap()];

        assert_eq!(check_dispute(&record, 1, 100), false);
    }
}
