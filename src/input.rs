use csv::StringRecord;

/// An `InputRecord` is used to store data from a single
/// row in the input CSV file.
#[derive(Debug, PartialEq)]
pub struct InputRecord {
    pub r#type: TransactionType,
    pub client: u16,
    pub tx: u32, // ideally this would be a type with more entropy such as a UUID.
    pub amount: Option<f64>,
}

/// All possible transaction types.
#[derive(Debug, Eq, PartialEq)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// This function processes each column in the incoming `StringRecord`.
/// If any column cannot be read, we return `None`. In a production
/// scenario, this would be coupled with logging and error handling
pub fn make_input_record(s_record: &StringRecord) -> Option<InputRecord> {
    let transaction_type = match s_record.get(0) {
        Some(s) => match s.to_lowercase().as_str() {
            "deposit" => TransactionType::Deposit,
            "withdrawal" => TransactionType::Withdrawal,
            "dispute" => TransactionType::Dispute,
            "resolve" => TransactionType::Resolve,
            "chargeback" => TransactionType::Chargeback,
            // If none of the above 5 transaction types were seen, this
            // is an invalid row and cannot be further processed
            _ => return None,
        },
        None => return None, // If the transaction type field is empty,
                             // this is an invalid row and cannot be
                             // further processed
    };

    // Check that the number of columns in the row
    // is correct. We should always have 4 columns,
    // regardless of transaction type.
    match transaction_type {
        TransactionType::Deposit
        | TransactionType::Withdrawal
        | TransactionType::Dispute
        | TransactionType::Resolve
        | TransactionType::Chargeback => match s_record.len() {
            4 => (),
            _ => return None,
        },
    }

    let client_id = match s_record.get(1) {
        Some(s) => match s.parse::<u16>() {
            Ok(s) => s,
            _ => return None, // If the client ID could not
                              // be parsed as a `u16`, the column
                              // must have invalid data in it.
                              // The row cannot be processed
                              // any further.
        },
        None => return None, // If the client ID field is empty,
                             // this is an invalid row and cannot be
                             // further processed
    };

    let transaction_id = match s_record.get(2) {
        Some(s) => match s.parse::<u32>() {
            Ok(s) => s,
            _ => return None, // If the transaction ID could not
                              // be parsed as a `u32`, the column
                              // must have invalid data in it.
                              // The row cannot be processed
                              // any further.
        },
        None => return None, // If the transaction ID field is empty,
                             // this is an invalid row and cannot be
                             // further processed
    };

    let amount = match s_record.get(3) {
        Some(s) => match s.parse::<f64>() {
            Ok(s) => Some(s),
            // If the amount could not be parsed as an `f64`,
            // check to see what type of transaction this is.
            // If it's a transaction type that does not require
            // an amount, the amount is simply `None`. Anything
            // else means that the row is invalid and cannot be
            // processed any further.
            _ => match transaction_type {
                TransactionType::Dispute
                | TransactionType::Resolve
                | TransactionType::Chargeback => None,
                _ => return None,
            },
        },
        // If the amount is empty, check to see what type of
        // transaction this is. If it's a transaction type
        // that does not require an amount, the amount is
        // simply `None`. Anything else means that the row
        // is invalid and cannot be processed any further.
        None => match transaction_type {
            TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => {
                None
            }
            _ => return None,
        },
    };

    // If we've made it this far, all columns in the row
    // were processed successfully. Use the extracted data
    // to build an `InputRecord` and return it.
    let res = InputRecord {
        r#type: transaction_type,
        client: client_id,
        tx: transaction_id,
        amount: amount,
    };

    Some(res)
}

#[cfg(test)]
pub mod tests {
    use super::{make_input_record, InputRecord, TransactionType};
    use csv::StringRecord;

    #[test]
    fn test_valid_deposit_record() {
        let record = StringRecord::from(vec!["deposit", "1", "1", "20.00"]);
        let test_record: InputRecord = InputRecord {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(20.00),
        };
        assert_eq!(make_input_record(&record), Some(test_record));
    }

    #[test]
    fn test_big_float_deposit_record() {
        let record = StringRecord::from(vec!["deposit", "1", "1", "20.987654321"]);
        let test_record: InputRecord = InputRecord {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(20.987654321),
        };
        assert_eq!(make_input_record(&record), Some(test_record));
    }

    #[test]
    fn test_valid_withdrawal_record() {
        let record = StringRecord::from(vec!["withdrawal", "1", "1", "20.00"]);
        let test_record: InputRecord = InputRecord {
            r#type: TransactionType::Withdrawal,
            client: 1,
            tx: 1,
            amount: Some(20.00),
        };
        assert_eq!(make_input_record(&record), Some(test_record));
    }

    #[test]
    fn test_valid_dispute_record() {
        let record = StringRecord::from(vec!["dispute", "1", "1", ""]);
        let test_record: InputRecord = InputRecord {
            r#type: TransactionType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
        };
        assert_eq!(make_input_record(&record), Some(test_record));
    }

    #[test]
    fn test_valid_resolve_record() {
        let record = StringRecord::from(vec!["resolve", "1", "1", ""]);
        let test_record: InputRecord = InputRecord {
            r#type: TransactionType::Resolve,
            client: 1,
            tx: 1,
            amount: None,
        };
        assert_eq!(make_input_record(&record), Some(test_record));
    }

    #[test]
    fn test_valid_chargeback_record() {
        let record = StringRecord::from(vec!["chargeback", "1", "1", ""]);
        let test_record: InputRecord = InputRecord {
            r#type: TransactionType::Chargeback,
            client: 1,
            tx: 1,
            amount: None,
        };
        assert_eq!(make_input_record(&record), Some(test_record));
    }

    #[test]
    fn test_record_empty_transaction_type_field() {
        let record = StringRecord::from(vec!["", "1", "1", "20.00"]);
        assert_eq!(make_input_record(&record), None);
    }

    #[test]
    fn test_record_empty_client_id_field() {
        let record = StringRecord::from(vec!["deposit", "", "1", "20.00"]);
        assert_eq!(make_input_record(&record), None);
    }

    #[test]
    fn test_record_empty_transaction_id_field() {
        let record = StringRecord::from(vec!["deposit", "1", "", "20.00"]);
        assert_eq!(make_input_record(&record), None);
    }

    #[test]
    fn test_record_empty_amount_field() {
        let record = StringRecord::from(vec!["deposit", "1", "1", ""]);
        assert_eq!(make_input_record(&record), None);
    }

    #[test]
    fn test_record_missing_transaction_type_field() {
        let record = StringRecord::from(vec!["1", "1", "20.00"]);
        assert_eq!(make_input_record(&record), None);
    }

    #[test]
    fn test_record_missing_client_id_field() {
        let record = StringRecord::from(vec!["deposit", "1", "20.00"]);
        assert_eq!(make_input_record(&record), None);
    }

    #[test]
    fn test_record_missing_transaction_id_field() {
        let record = StringRecord::from(vec!["deposit", "1", "20.00"]);
        assert_eq!(make_input_record(&record), None);
    }

    #[test]
    fn test_record_missing_amount_field() {
        let record = StringRecord::from(vec!["deposit", "1", "1"]);
        assert_eq!(make_input_record(&record), None);
    }
}
