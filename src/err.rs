// use thiserror::Error;

// #[derive(Error, Debug)]
// pub enum StorageError {
//     #[error("SQLite error: {0}")]
//     Sqlite(#[from] rusqlite::Error),
//     #[error("Encoding error: {0}")]
//     Encoding(String),
//     #[error("Decoding error: {0}")]
//     Decoding(String),
//     #[error("Key not found: {0}")]
//     KeyNotFound(String),
//     #[error("Value expired")]
//     Expired,
// }
