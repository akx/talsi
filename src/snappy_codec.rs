use crate::data_codecs::DataToDataCodec;
use crate::typ::DataAndMnemonic;
use pyo3::PyResult;
use std::io::{Read, Write};
use tracing::instrument;

pub(crate) struct SnappyCodec;

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
