use either::Either;
use pyo3::types::{PyBytes, PyBytesMethods, PyString, PyStringMethods};
use pyo3::{Bound, Py, PyAny, PyResult};
use smallvec::{SmallVec, smallvec};
use std::ops::Deref;

pub(crate) type StringOrByteString = Either<String, Vec<u8>>;

pub(crate) type CodecMnemonic = u8;

pub(crate) const CODECS_BLOB_CAPACITY: usize = 8;

pub(crate) type CodecsBlob = SmallVec<[CodecMnemonic; CODECS_BLOB_CAPACITY]>;
pub(crate) struct DataAndMnemonic {
    pub data: Vec<u8>,
    pub codec: CodecMnemonic,
}

/// The contents of an immutable Python object (`bytes` or `str`), readable without
/// holding the GIL or copying.
pub(crate) struct PyBorrowedBytes {
    // Keeps the object (and thus the buffer `ptr` points into) alive.
    _owner: Py<PyAny>,
    ptr: *const u8,
    len: usize,
}

// SAFETY: The buffer belongs to an immutable object we hold a strong reference to,
// so it can neither change nor be freed while we're around, on any thread.
unsafe impl Send for PyBorrowedBytes {}
unsafe impl Sync for PyBorrowedBytes {}

impl PyBorrowedBytes {
    pub fn from_bytes(obj: &Bound<PyBytes>) -> Self {
        let data = obj.as_bytes();
        Self {
            ptr: data.as_ptr(),
            len: data.len(),
            _owner: obj.clone().into_any().unbind(),
        }
    }
    /// Borrows the UTF-8 representation of `obj`, which CPython caches on the object
    /// for its lifetime once created.
    pub fn from_str(obj: &Bound<PyString>) -> PyResult<Self> {
        let data = obj.to_str()?.as_bytes();
        Ok(Self {
            ptr: data.as_ptr(),
            len: data.len(),
            _owner: obj.clone().into_any().unbind(),
        })
    }
}

impl Deref for PyBorrowedBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: See the `Send`/`Sync` impls above.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Encoded data, either owned or borrowed from a Python object.
pub(crate) enum Payload {
    Owned(Vec<u8>),
    Python(PyBorrowedBytes),
}

impl Deref for Payload {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Payload::Owned(v) => v,
            Payload::Python(b) => b,
        }
    }
}

/// The result of encoding a Python object.
pub(crate) struct PyEncoded {
    pub data: Payload,
    pub codec: CodecMnemonic,
}

pub(crate) struct DataAndMnemonics {
    pub data: Payload,
    pub codecs: CodecsBlob,
}

impl DataAndMnemonics {
    pub fn from_single(pe: PyEncoded) -> Self {
        let PyEncoded { data, codec } = pe;
        Self {
            data,
            codecs: smallvec![codec],
        }
    }
    pub fn from_data(data: Payload, mnemonic: CodecMnemonic) -> Self {
        Self {
            data,
            codecs: smallvec![mnemonic],
        }
    }
    pub fn from_two(data: Vec<u8>, mnemonic1: CodecMnemonic, mnemonic2: CodecMnemonic) -> Self {
        Self {
            data: Payload::Owned(data),
            codecs: smallvec![mnemonic1, mnemonic2],
        }
    }
}
