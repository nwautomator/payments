# Simple Transaction Processor

This repository illustrates how a simple payments engine might be written using the Rust programming language. It makes use of only two crates: the [CSV crate](https://crates.io/crates/csv) and the very popular [Serde crate](https://crates.io/crates/serde).

Comments and docstrings are included in order to make the code and design decisions easy to understand.

## Running the code

In order to run this, you'll need a current version of Rust installed. See the [Rustup](https://rustup.rs/) for installation instructions.

Create an input CSV file with the name of your choice. It should have the following columns:

```{.shell}
type, client, tx, amount
```

The rows should contain a transaction type, a client ID, a transaction ID, and a decimal amount. See [input.rs](src/input.rs) for more detail. With the file created, simply run:

```{.shell}
cargo run -q -- <name of input file.csv>
```

You should see the account balance for each client in the output. The output can also be piped to a CSV file if you wish to save the results.
