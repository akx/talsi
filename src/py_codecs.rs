use crate::typ::DataAndMnemonic;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::{PyAnyMethods, PyStringMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyCFunction, PyFunction, PyString};
use pyo3::{Bound, Py, PyAny, PyErr, PyResult, Python};
use tracing::instrument;

trait PythonToDataCodec {
    fn encode(py: Python, obj: &Bound<PyAny>) -> PyResult<DataAndMnemonic>;
    fn decode<'a>(py: Python<'a>, data: &[u8]) -> PyResult<Bound<'a, PyAny>>;
    const MNEMONIC: u8;
}

struct PickleCodec;
impl PythonToDataCodec for PickleCodec {
    #[instrument(skip_all, name = "pickle_encode")]
    fn encode(py: Python, obj: &Bound<PyAny>) -> PyResult<DataAndMnemonic> {
        static PICKLE_DUMPS: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
        let bytes = PICKLE_DUMPS
            .import(py, "pickle", "dumps")?
            .call1((obj, 4))?;
        Ok(DataAndMnemonic {
            data: bytes.extract()?,
            codec: Self::MNEMONIC,
        })
    }
    #[instrument(skip_all, name = "pickle_decode")]
    fn decode<'a>(py: Python<'a>, data: &[u8]) -> PyResult<Bound<'a, PyAny>> {
        static PICKLE_LOADS: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
        unsafe {
            let bytes = PyBytes::from_ptr(py, data.as_ptr() as *mut u8, data.len());
            Ok(PICKLE_LOADS
                .import(py, "pickle", "loads")?
                .call1((bytes,))?
                .into_any())
        }
    }

    const MNEMONIC: u8 = b'P';
}

struct JsonCodec;
impl PythonToDataCodec for JsonCodec {
    #[instrument(skip_all, name = "json_encode")]
    fn encode(py: Python, obj: &Bound<PyAny>) -> PyResult<DataAndMnemonic> {
        static JSON_DUMPS: PyOnceLock<Py<PyFunction>> = PyOnceLock::new();
        let str = JSON_DUMPS.import(py, "json", "dumps")?.call1((obj,))?;
        let bytes = str.extract::<String>()?.into_bytes();
        Ok(DataAndMnemonic {
            data: bytes,
            codec: Self::MNEMONIC,
        })
    }
    #[instrument(skip_all, name = "json_decode")]
    fn decode<'a>(py: Python<'a>, data: &[u8]) -> PyResult<Bound<'a, PyAny>> {
        // Try to use orjson, if it's available, for JSON decoding.
        static ORJSON_LOADS: PyOnceLock<Option<Py<PyCFunction>>> = PyOnceLock::new();
        let orjson_loads = ORJSON_LOADS.get_or_try_init(py, || {
            // Ignore import errors, but fail if getattr/cast fails
            if let Ok(orjson_mod) = py.import("orjson") {
                return Ok::<Option<Py<PyCFunction>>, PyErr>(Some(
                    orjson_mod
                        .getattr("loads")?
                        .cast_into::<PyCFunction>()?
                        .unbind(),
                ));
            }
            Ok(None)
        })?;
        if let Some(orjson_loads) = orjson_loads {
            unsafe {
                let bytes = PyBytes::from_ptr(py, data.as_ptr() as *mut u8, data.len());
                return Ok(orjson_loads.bind(py).call1((bytes,))?.into_any());
            }
        }

        static JSON_LOADS: PyOnceLock<Py<PyFunction>> = PyOnceLock::new();
        unsafe {
            let bytes = PyBytes::from_ptr(py, data.as_ptr() as *mut u8, data.len());
            Ok(JSON_LOADS
                .import(py, "json", "loads")?
                .call1((bytes,))?
                .into_any())
        }
    }

    const MNEMONIC: u8 = b'J';
}

struct BytesCodec;
impl PythonToDataCodec for BytesCodec {
    #[instrument(skip_all, name = "bytes_encode")]
    fn encode(_py: Python, obj: &Bound<PyAny>) -> PyResult<DataAndMnemonic> {
        if obj.is_instance_of::<PyBytes>() {
            Ok(DataAndMnemonic {
                data: obj.extract::<Vec<u8>>()?,
                codec: Self::MNEMONIC,
            })
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected bytes object"))
        }
    }
    #[instrument(skip_all, name = "bytes_decode")]
    fn decode<'a>(py: Python<'a>, data: &[u8]) -> PyResult<Bound<'a, PyAny>> {
        let bytes = PyBytes::new(py, data);
        Ok(bytes.into_any())
    }

    const MNEMONIC: u8 = b'B';
}
struct UTF8Codec;
impl PythonToDataCodec for UTF8Codec {
    #[instrument(skip_all, name = "utf8_encode")]
    fn encode(_py: Python, obj: &Bound<PyAny>) -> PyResult<DataAndMnemonic> {
        let str = obj.cast::<PyString>()?;
        Ok(DataAndMnemonic {
            data: Vec::from(str.to_str()?),
            codec: Self::MNEMONIC,
        })
    }
    #[instrument(skip_all, name = "utf8_decode")]
    fn decode<'a>(py: Python<'a>, data: &[u8]) -> PyResult<Bound<'a, PyAny>> {
        let bytes = PyString::new(py, String::from_utf8_lossy(data).as_ref());
        Ok(bytes.into_any())
    }

    const MNEMONIC: u8 = b'U';
}

#[instrument(skip_all)]
pub fn get_best_py_encoding(
    py: Python,
    obj: &Bound<PyAny>,
    allow_pickle: bool,
) -> PyResult<DataAndMnemonic> {
    if obj.is_instance_of::<PyString>() {
        return UTF8Codec::encode(py, obj);
    }
    if obj.is_instance_of::<PyBytes>() {
        return BytesCodec::encode(py, obj);
    }
    if allow_pickle {
        return PickleCodec::encode(py, obj);
    }
    JsonCodec::encode(py, obj)
}

#[instrument(skip_all)]
pub fn decode_to_python_from_data_and_mnemonic(
    py: Python,
    data_and_mnemonic: DataAndMnemonic,
    allow_pickle: bool,
) -> PyResult<Bound<PyAny>> {
    let DataAndMnemonic {
        data,
        codec: mnemonic,
    } = data_and_mnemonic;
    match mnemonic {
        PickleCodec::MNEMONIC => {
            if allow_pickle {
                PickleCodec::decode(py, &data)
            } else {
                Err(PyErr::new::<PyTypeError, _>("Pickle encoding not allowed"))
            }
        }
        JsonCodec::MNEMONIC => JsonCodec::decode(py, &data),
        BytesCodec::MNEMONIC => BytesCodec::decode(py, &data),
        UTF8Codec::MNEMONIC => UTF8Codec::decode(py, &data),
        _ => Err(PyErr::new::<PyTypeError, _>(format!(
            "Unknown Python encoding mnemonic: {}",
            { mnemonic }
        ))),
    }
}
