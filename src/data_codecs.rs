use crate::snappy_codec::SnappyCodec;
use crate::typ::DataAndMnemonic;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, PyResult};
use tracing::instrument;

pub(crate) trait DataToDataCodec {
    fn encode(&self, data: &[u8]) -> PyResult<DataAndMnemonic>;
    fn decode(&self, data: &[u8]) -> PyResult<Vec<u8>>;
    const MNEMONIC: u8;
}

#[instrument(skip_all)]
pub fn get_best_data_encoding(data: &[u8]) -> PyResult<Option<DataAndMnemonic>> {
    if data.len() >= 1024 {
        return SnappyCodec.encode(data).map(Some);
    }
    Ok(None)
}

pub fn decode_from_data_and_mnemonic(data_and_mnemonic: DataAndMnemonic) -> PyResult<Vec<u8>> {
    let DataAndMnemonic {
        data,
        codec: mnemonic,
    } = data_and_mnemonic;
    match mnemonic {
        SnappyCodec::MNEMONIC => SnappyCodec.decode(&data),
        _ => Err(PyErr::new::<PyValueError, _>(format!(
            "Unknown data encoding mnemonic: {}",
            { mnemonic }
        ))),
    }
}
