use crate::typ::DataAndMnemonic;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, PyResult};
use std::io::{Read, Write};
use tracing::instrument;

trait DataToDataCodec {
    fn encode(&self, data: &[u8]) -> PyResult<DataAndMnemonic>;
    fn decode(&self, data: &[u8]) -> PyResult<Vec<u8>>;
    const MNEMONIC: u8;
}
struct SnappyCodec;
impl DataToDataCodec for SnappyCodec {
    #[instrument(name = "snappy_encode", skip_all)]
    fn encode(&self, data: &[u8]) -> PyResult<DataAndMnemonic> {
        let mut wtr = snap::write::FrameEncoder::new(Vec::with_capacity(data.len() / 2));
        wtr.write_all(data)?;
        let compressed = wtr.into_inner().unwrap();
        Ok(DataAndMnemonic {
            data: compressed,
            codec: Self::MNEMONIC,
        })
    }

    #[instrument(name = "snappy_decode", skip_all)]
    fn decode(&self, data: &[u8]) -> PyResult<Vec<u8>> {
        let mut rdr = snap::read::FrameDecoder::new(data);
        let mut decompressed = Vec::new();
        rdr.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    const MNEMONIC: u8 = b's';
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
