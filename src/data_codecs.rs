use crate::snappy_codec::SnappyCodec;
use crate::typ::DataAndMnemonic;
use crate::utils::to_talsi_error;
use crate::zstd_codec::ZstdCodec;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, PyResult};
use tracing::instrument;

pub(crate) trait DataToDataCodec {
    fn encode(&self, data: &[u8]) -> PyResult<DataAndMnemonic>;
    fn decode(&self, data: &[u8]) -> PyResult<Vec<u8>>;
    const MNEMONIC: u8;
}

#[derive(Clone, Copy)]
pub(crate) enum CompressionAlgorithm {
    Snappy,
    Zstd { level: i32 },
}

impl CompressionAlgorithm {
    pub(crate) fn from_str(compression: &str) -> PyResult<CompressionAlgorithm> {
        if compression == "snappy" {
            return Ok(CompressionAlgorithm::Snappy);
        }
        if compression == "zstd" {
            return Ok(CompressionAlgorithm::Zstd { level: 3 });
        }
        if let Some(level_str) = compression.strip_prefix("zstd:") {
            let level = level_str.parse::<i32>().map_err(|_| {
                to_talsi_error(format!("Invalid zstd compression level: {}", level_str))
            })?;
            if !(1..=22).contains(&level) {
                return Err(to_talsi_error(format!(
                    "Zstd compression level must be between 1 and 22, got: {}",
                    level
                )));
            }
            return Ok(CompressionAlgorithm::Zstd { level });
        }
        Err(to_talsi_error(format!(
            "Unknown compression algorithm: {}. Use 'snappy', 'zstd', or 'zstd:LEVEL'",
            compression
        )))
    }
}

#[instrument(skip_all)]
pub fn get_best_data_encoding(
    data: &[u8],
    algorithm: CompressionAlgorithm,
) -> PyResult<Option<DataAndMnemonic>> {
    if data.len() >= 1024 {
        return match algorithm {
            CompressionAlgorithm::Snappy => SnappyCodec.encode(data).map(Some),
            CompressionAlgorithm::Zstd { level } => ZstdCodec::new(level).encode(data).map(Some),
        };
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
        ZstdCodec::MNEMONIC => ZstdCodec::new_default().decode(&data),
        _ => Err(PyErr::new::<PyValueError, _>(format!(
            "Unknown data encoding mnemonic: {}",
            { mnemonic }
        ))),
    }
}
