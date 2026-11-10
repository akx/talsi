use crate::typ::{DataAndMnemonic, Payload, PyBorrowedBytes, PyEncoded};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyCFunction, PyFunction, PyString};
use pyo3::{Bound, Py, PyAny, PyErr, PyResult, Python};
use tracing::instrument;

trait PythonToDataCodec {
    fn encode(py: Python, obj: &Bound<PyAny>) -> PyResult<PyEncoded>;
    fn decode<'a>(py: Python<'a>, data: &[u8]) -> PyResult<Bound<'a, PyAny>>;
    const MNEMONIC: u8;
}

struct PickleCodec;
impl PythonToDataCodec for PickleCodec {
    #[instrument(skip_all, name = "pickle_encode")]
    fn encode(py: Python, obj: &Bound<PyAny>) -> PyResult<PyEncoded> {
        static PICKLE_DUMPS: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
        let bytes = PICKLE_DUMPS
            .import(py, "pickle", "dumps")?
            .call1((obj, 4))?;
        Ok(PyEncoded {
            data: Payload::Python(PyBorrowedBytes::from_bytes(bytes.cast::<PyBytes>()?)),
            codec: Self::MNEMONIC,
        })
    }
    #[instrument(skip_all, name = "pickle_decode")]
    fn decode<'a>(py: Python<'a>, data: &[u8]) -> PyResult<Bound<'a, PyAny>> {
        static PICKLE_LOADS: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
        Ok(PICKLE_LOADS
            .import(py, "pickle", "loads")?
            .call1((PyBytes::new(py, data),))?
            .into_any())
    }

    const MNEMONIC: u8 = b'P';
}

struct JsonCodec;
impl PythonToDataCodec for JsonCodec {
    #[instrument(skip_all, name = "json_encode")]
    fn encode(py: Python, obj: &Bound<PyAny>) -> PyResult<PyEncoded> {
        static JSON_DUMPS: PyOnceLock<Py<PyFunction>> = PyOnceLock::new();
        let str = JSON_DUMPS.import(py, "json", "dumps")?.call1((obj,))?;
        Ok(PyEncoded {
            data: Payload::Python(PyBorrowedBytes::from_str(str.cast::<PyString>()?)?),
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
            return Ok(orjson_loads
                .bind(py)
                .call1((PyBytes::new(py, data),))?
                .into_any());
        }

        static JSON_LOADS: PyOnceLock<Py<PyFunction>> = PyOnceLock::new();
        Ok(JSON_LOADS
            .import(py, "json", "loads")?
            .call1((PyBytes::new(py, data),))?
            .into_any())
    }

    const MNEMONIC: u8 = b'J';
}

struct BytesCodec;
impl PythonToDataCodec for BytesCodec {
    #[instrument(skip_all, name = "bytes_encode")]
    fn encode(_py: Python, obj: &Bound<PyAny>) -> PyResult<PyEncoded> {
        let bytes = obj
            .cast::<PyBytes>()
            .map_err(|_| PyErr::new::<PyTypeError, _>("Expected bytes object"))?;
        Ok(PyEncoded {
            data: Payload::Python(PyBorrowedBytes::from_bytes(bytes)),
            codec: Self::MNEMONIC,
        })
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
    fn encode(_py: Python, obj: &Bound<PyAny>) -> PyResult<PyEncoded> {
        Ok(PyEncoded {
            data: Payload::Python(PyBorrowedBytes::from_str(obj.cast::<PyString>()?)?),
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
) -> PyResult<PyEncoded> {
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
