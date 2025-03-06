mod data_codecs;
mod py_codecs;
mod storage;
mod typ;

#[cfg(feature = "tracing")]
use tracing_subscriber;
#[cfg(feature = "tracing")]
use tracing_subscriber::layer::SubscriberExt;
#[cfg(feature = "tracing")]
use tracing_tree;

use pyo3::prelude::*;

use pyo3::create_exception;

create_exception!(talsi, TalsiError, pyo3::exceptions::PyException);

#[pyfunction]
fn setup_logging() {
    #[cfg(feature = "tracing")]
    {
        let subscriber = tracing_subscriber::Registry::default().with(
            tracing_tree::HierarchicalLayer::new(2)
                .with_ansi(true)
                .with_thread_ids(true)
                .with_timer(tracing_tree::time::Uptime::default()),
        );
        tracing::subscriber::set_global_default(subscriber).unwrap();
        info!("Logging initialized");
    }
}

#[pymodule]
fn talsi(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<storage::Storage>()?;
    m.add("TalsiError", py.get_type::<TalsiError>())?;
    m.add_function(wrap_pyfunction!(setup_logging, m)?)?;
    Ok(())
}
