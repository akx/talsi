use crate::data_codecs::DataToDataCodec;
use crate::typ::DataAndMnemonic;
use crate::utils::to_talsi_error;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, PyResult};
use std::cell::RefCell;
use tracing::instrument;
use zstd::bulk::Compressor;

const MAX_ZSTD_LEVEL: usize = 22; // There is no const API to get this

type CompressorsRefCell = RefCell<[Option<Compressor<'static>>; MAX_ZSTD_LEVEL + 1]>;

thread_local! {
    static ZSTD_ENCODERS: CompressorsRefCell = RefCell::new([const { None }; MAX_ZSTD_LEVEL + 1]);
}

pub(crate) struct ZstdCodec {
    level: i32,
}

impl ZstdCodec {
    pub fn new_default() -> Self {
        ZstdCodec { level: 3 } // 3 chosen by fair dice roll
    }
    pub fn new(level: i32) -> Self {
        if level < 1 || level > (MAX_ZSTD_LEVEL as i32) {
            // We should never reach this point if used correctly;
            // making this `new` return a Result is more of a hassle.
            panic!("zstd codec: level {} out of range", level);
        }
        ZstdCodec { level }
    }

    fn encode_with_cached_encoder(
        &self,
        encoders: &CompressorsRefCell,
        data: &[u8],
    ) -> PyResult<Vec<u8>> {
        let mut encoders = encoders.borrow_mut();
        let level_idx = self.level as usize; // Validated in `new`
        let encoder = match &mut encoders[level_idx] {
            Some(encoder) => encoder,
            None => encoders[level_idx].insert(Compressor::new(self.level).map_err(|e| {
                PyErr::new::<PyValueError, _>(format!("Failed to create Zstd encoder: {}", e))
            })?),
        };

        encoder
            .compress(data)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("Zstd compression failed: {}", e)))
    }
}

impl DataToDataCodec for ZstdCodec {
    #[instrument(name = "zstd_encode", skip_all)]
    fn encode(&self, data: &[u8]) -> PyResult<DataAndMnemonic> {
        let compressed = ZSTD_ENCODERS
            .try_with(|encoders| self.encode_with_cached_encoder(encoders, data))
            .map_err(to_talsi_error)??;

        Ok(DataAndMnemonic {
            data: compressed,
            codec: Self::MNEMONIC,
        })
    }

    #[instrument(name = "zstd_decode", skip_all)]
    fn decode(&self, data: &[u8]) -> PyResult<Vec<u8>> {
        let decompressed = zstd::decode_all(data)?;
        Ok(decompressed)
    }

    const MNEMONIC: u8 = b'z';
}
