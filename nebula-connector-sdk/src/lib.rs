//! The public connector wire contract. Validate every batch at the transport boundary.

#[allow(
    clippy::possible_missing_else,
    reason = "Typify emits independent validation conditions on one line"
)]
pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}

pub mod validation;
pub use validation::{InvalidBatch, ValidatedBatch};

pub mod client;
pub use client::{Client, ClientError, Receipt, Run};

pub mod connector;
pub use connector::{Connector, ConnectorError, Context, Page, Specification};

pub mod runner;
pub mod scope;
