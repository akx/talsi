use crate::TalsiError;
use pyo3::PyErr;

#[inline]
pub(crate) fn to_talsi_error<T: ToString>(e: T) -> PyErr {
    PyErr::new::<TalsiError, _>(e.to_string())
}
