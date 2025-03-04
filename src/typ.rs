use either::Either;
use smallvec::{smallvec, SmallVec};

pub(crate) type StringOrByteString = Either<String, Vec<u8>>;

pub(crate) type CodecMnemonic = u8;

pub(crate) const CODECS_BLOB_CAPACITY: usize = 8;

pub(crate) type CodecsBlob = SmallVec<[CodecMnemonic; CODECS_BLOB_CAPACITY]>;
pub(crate) struct DataAndMnemonic {
    pub data: Vec<u8>,
    pub codec: CodecMnemonic,
}

pub(crate) struct DataAndMnemonics {
    pub data: Vec<u8>,
    pub codecs: CodecsBlob,
}

impl DataAndMnemonics {
    pub fn from_single(dm: DataAndMnemonic) -> Self {
        let DataAndMnemonic { data, codec } = dm;
        Self {
            data,
            codecs: smallvec![codec],
        }
    }
    pub fn from_data(data: Vec<u8>, mnemonic: CodecMnemonic) -> Self {
        Self {
            data,
            codecs: smallvec![mnemonic],
        }
    }
    pub fn from_two(data: Vec<u8>, mnemonic1: CodecMnemonic, mnemonic2: CodecMnemonic) -> Self {
        Self {
            data,
            codecs: smallvec![mnemonic1, mnemonic2],
        }
    }
}
