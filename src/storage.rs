use crate::data_codecs::{decode_from_data_and_mnemonic, get_best_data_encoding};
use crate::py_codecs::{decode_to_python_from_data_and_mnemonic, get_best_py_encoding};
use crate::typ::{CodecsBlob, DataAndMnemonic, DataAndMnemonics, StringOrByteString};
use crate::TalsiError;
use either::Either;
use eyre::Context;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFrozenSet};
use pyo3::{pyclass, pymethods, Bound, Py, PyAny, PyErr, PyObject, PyResult, Python};
use rayon::prelude::*;
use rusqlite::limits::Limit;
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection};
use rusqlite::{params_from_iter, OptionalExtension};
use std::collections::HashSet;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::instrument;

fn string_or_bytestring_as_string(sobs: StringOrByteString) -> eyre::Result<String> {
    match sobs {
        Either::Left(s) => Ok(s.to_string()),
        Either::Right(b) => Ok(std::str::from_utf8(&b)
            .wrap_err("bytestring didn't parse as utf-8")?
            .to_string()),
    }
}

fn string_or_bytestring_vector_as_string_vector(
    sobses: Vec<StringOrByteString>,
) -> eyre::Result<Vec<String>> {
    let mut res = vec![];
    for sobs in sobses {
        res.push(string_or_bytestring_as_string(sobs)?);
    }
    Ok(res)
}

struct StorageSettings {
    pub allow_pickle: bool,
}

#[pyclass]
pub struct Storage {
    conn: Mutex<Option<Connection>>,
    known_namespaces: RwLock<HashSet<String>>,
    settings: StorageSettings,
    max_num_binds: usize,
}

struct InternalInsertTriple {
    key: String,
    codecs_blob: CodecsBlob,
    value: Vec<u8>,
}

struct InternalStoredRecord {
    key: Option<String>, // we may have not queried this
    codecs_blob: CodecsBlob,
    value: Vec<u8>,
    expires_at_ms: Option<i64>,
}
struct InternalStoredDataAndMnemonic {
    key: Option<String>, // we may have not queried this
    #[allow(dead_code)]
    expires_at_ms: Option<i64>,
    data_and_mnemonic: DataAndMnemonic,
}

impl InternalStoredDataAndMnemonic {
    fn into_python<'py>(
        self,
        py: Python<'py>,
        s: &StorageSettings,
    ) -> PyResult<(Option<String>, Bound<'py, PyAny>)> {
        let py_val =
            decode_to_python_from_data_and_mnemonic(py, self.data_and_mnemonic, s.allow_pickle)?;
        Ok((self.key, py_val))
    }
}

impl InternalStoredRecord {
    fn into_data_codecs_decoded(self) -> PyResult<InternalStoredDataAndMnemonic> {
        let mut value = self.value;
        let (python_codec_mnemonic, data_codecs) = self
            .codecs_blob
            .split_first()
            .ok_or_else(|| argh("No codec mnemonic found"))?;
        if !data_codecs.is_empty() {
            // Decode data codecs in reverse order
            for mnemonic in data_codecs.iter().rev() {
                value = decode_from_data_and_mnemonic(DataAndMnemonic {
                    data: value,
                    codec: *mnemonic,
                })?;
            }
        }
        Ok(InternalStoredDataAndMnemonic {
            key: self.key,
            data_and_mnemonic: DataAndMnemonic {
                data: value,
                codec: *python_codec_mnemonic,
            },
            expires_at_ms: self.expires_at_ms,
        })
    }
}

#[inline]
fn argh<T: ToString>(e: T) -> PyErr {
    PyErr::new::<TalsiError, _>(e.to_string())
}

fn ensure_namespace_table(conn: &Connection, namespace: &str) -> Result<(), PyErr> {
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS tl_{} (
                    key TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 0,
                    codecs BLOB NOT NULL,
                    value BLOB NOT NULL,
                    created_at_ms TIMESTAMP NOT NULL,
                    expires_at_ms TIMESTAMP,
                    PRIMARY KEY (key, version)
                )",
            namespace
        ),
        [],
    )
    .map_err(argh)?;
    conn.execute(
        &format!(
            "CREATE INDEX IF NOT EXISTS tl_{}_key ON tl_{} (key)",
            namespace, namespace
        ),
        [],
    )
    .map_err(argh)?;
    Ok(())
}

impl Storage {
    fn ensure_namespace_table(&self, conn_lock: &Connection, namespace: &str) -> Result<(), PyErr> {
        let known_namespaces = self.known_namespaces.read().unwrap();
        // If we've already created the table, don't do it again.
        if known_namespaces.contains(namespace) {
            return Ok(());
        }
        drop(known_namespaces);
        let mut known_namespaces = self.known_namespaces.write().unwrap();
        ensure_namespace_table(conn_lock, namespace)?;
        known_namespaces.insert(namespace.to_string());
        Ok(())
    }

    #[inline]
    #[instrument(skip_all)]
    fn internal_insert(
        &self,
        namespace: &str,
        now: Duration,
        expires_at: Option<Duration>,
        iits: &[InternalInsertTriple],
    ) -> Result<(), PyErr> {
        let now_ms = now.as_millis() as i64;
        let expires_ms = expires_at.map(|t| t.as_millis() as i64);
        let maybe_conn = self.conn.lock().unwrap();
        let conn = maybe_conn
            .as_ref()
            .ok_or_else(|| argh("Connection is closed"))?;
        self.ensure_namespace_table(conn, namespace)?;
        let tx = conn.unchecked_transaction().map_err(argh)?;
        let mut stmt = tx
            .prepare_cached(&format!("INSERT OR REPLACE INTO tl_{} (key, codecs, value, created_at_ms, expires_at_ms) VALUES (?, ?, ?, ?, ?)", namespace))
            .map_err(argh)?;
        for iit in iits {
            let InternalInsertTriple {
                key,
                codecs_blob,
                value: data_encoded,
            } = iit;
            stmt.execute(params![
                key,
                codecs_blob.as_slice(),
                data_encoded,
                now_ms,
                expires_ms
            ])
            .map_err(argh)?;
        }
        drop(stmt);
        tx.commit().map_err(argh)?;
        Ok(())
    }

    #[inline]
    #[instrument(skip_all)]
    fn internal_delete(&self, namespace: String, keys: &[String]) -> PyResult<usize> {
        let maybe_conn = self.conn.lock().unwrap();
        let conn = maybe_conn
            .as_ref()
            .ok_or_else(|| argh("Connection is closed"))?;
        let tx = conn.unchecked_transaction().map_err(argh)?;
        let mut n = 0;
        for keys in keys.chunks(self.max_num_binds) {
            let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = &format!(
                "DELETE FROM tl_{} WHERE key IN ({})",
                namespace, placeholders
            );
            match conn.execute(query, params_from_iter(keys.iter())) {
                Ok(rows) => {
                    n += rows;
                }
                Err(e) => {
                    if e.to_string().contains("no such table") {
                        return Ok(0);
                    } else {
                        return Err(argh(e));
                    }
                }
            }
        }
        tx.commit().map_err(argh)?;
        Ok(n)
    }
}

#[pymethods]
impl Storage {
    #[new]
    #[pyo3(signature = (path, *, allow_pickle = false))]
    fn new(path: &str, allow_pickle: bool) -> PyResult<Self> {
        let conn = Connection::open(path).map_err(argh)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .map_err(argh)?;
        let max_num_binds = conn
            .limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
            .map_err(argh)? as usize;

        Ok(Storage {
            conn: Mutex::new(Some(conn)),
            max_num_binds,
            known_namespaces: RwLock::new(HashSet::new()),
            settings: StorageSettings { allow_pickle },
        })
    }

    fn close(&self) -> PyResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let conn = conn.take();
        if let Some(conn) = conn {
            conn.close().ok();
        }
        Ok(())
    }

    #[pyo3(signature = (namespace, key, value, ttl_ms=None))]
    #[instrument(skip_all)]
    fn set(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        key: StringOrByteString,
        value: Py<PyAny>,
        ttl_ms: Option<u64>,
    ) -> PyResult<()> {
        let py_enc_result = get_best_py_encoding(py, value.bind(py), self.settings.allow_pickle)?;
        py.allow_threads(|| {
            let key = string_or_bytestring_as_string(key)?;
            let namespace = string_or_bytestring_as_string(namespace)?;
            let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(argh)?;
            let expires_at = ttl_ms.map(|ttl| now + Duration::from_millis(ttl));
            let data_enc_result = get_best_data_encoding(&py_enc_result.data)?;
            let DataAndMnemonics {
                data: data_encoded,
                codecs: codecs_blob,
            } = match data_enc_result {
                Some(data_enc_result) => DataAndMnemonics::from_two(
                    data_enc_result.data,
                    py_enc_result.codec,
                    data_enc_result.codec,
                ),
                None => DataAndMnemonics::from_single(py_enc_result), // didn't encode further
            };
            let iit = InternalInsertTriple {
                key,
                codecs_blob,
                value: data_encoded,
            };
            self.internal_insert(&namespace, now, expires_at, &[iit])?;
            Ok(())
        })
    }

    fn get(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        key: StringOrByteString,
    ) -> PyResult<Option<Py<PyAny>>> {
        let idd = py.allow_threads(|| -> PyResult<Option<InternalStoredDataAndMnemonic>> {
            let key = string_or_bytestring_as_string(key)?;
            let namespace = string_or_bytestring_as_string(namespace)?;
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn.as_ref().ok_or(argh("Connection is closed"))?;
            let mut stmt = conn
                .prepare_cached(&format!(
                    "SELECT value, codecs, expires_at_ms FROM tl_{} WHERE key = ? LIMIT 1",
                    namespace
                ))
                .map_err(argh)?;
            let isr = stmt
                .query_row(params![key], |row| {
                    let codecs_blob = match row.get_ref(1)? {
                        ValueRef::Blob(v) => CodecsBlob::from_slice(v),
                        _ => panic!("invalid codec blob type"),
                    };
                    Ok(InternalStoredRecord {
                        key: None,
                        value: row.get(0)?,
                        codecs_blob,
                        expires_at_ms: row.get(2)?,
                    })
                })
                .optional()
                .map_err(argh)?;
            match isr {
                Some(isr) => Ok(Some(isr.into_data_codecs_decoded()?)),
                None => Ok(None),
            }
        })?;
        match idd {
            Some(idd) => {
                // TODO: check expiry
                let (_, py_val) = idd.into_python(py, &self.settings)?;
                Ok(Some(py_val.into()))
            }
            None => Ok(None),
        }
    }

    fn has(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        key: StringOrByteString,
    ) -> PyResult<bool> {
        let key = string_or_bytestring_as_string(key)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        py.allow_threads(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| argh("Connection is closed"))?;
            let mut stmt = conn
                .prepare_cached(&format!(
                    "SELECT EXISTS(SELECT 1 FROM tl_{} WHERE key = ? LIMIT 1)",
                    namespace
                ))
                .map_err(argh)?;
            let exists: i64 = stmt
                .query_row(params![key], |row| row.get(0))
                .optional()
                .map_err(argh)?
                .unwrap_or(0);
            Ok::<bool, PyErr>(exists != 0)
        })
    }

    fn has_many(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        keys: Vec<StringOrByteString>,
    ) -> PyResult<Py<PyFrozenSet>> {
        let keys = string_or_bytestring_vector_as_string_vector(keys)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        let extant_keys = py.allow_threads(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| argh("Connection is closed"))?;
            let mut extant_keys: HashSet<String> = HashSet::with_capacity(keys.len());
            for keys in keys.chunks(self.max_num_binds) {
                let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query = format!(
                    "SELECT key FROM tl_{} WHERE key IN ({})",
                    namespace, placeholders
                );
                let mut stmt = conn.prepare(&query).map_err(argh)?;
                let keys = stmt
                    .query_map(params_from_iter(keys.iter()), |row| row.get(0))
                    .map_err(argh)?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(argh)?;
                extant_keys.extend(keys);
            }
            Ok::<HashSet<String>, PyErr>(extant_keys)
        })?;
        let fz = PyFrozenSet::new(py, extant_keys)?;
        Ok(fz.into())
    }

    fn delete(&self, namespace: StringOrByteString, key: StringOrByteString) -> PyResult<usize> {
        let key = string_or_bytestring_as_string(key)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        self.internal_delete(namespace, &[key])
    }

    fn delete_many(
        &self,
        namespace: StringOrByteString,
        keys: Vec<StringOrByteString>,
    ) -> PyResult<usize> {
        let keys = string_or_bytestring_vector_as_string_vector(keys)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        self.internal_delete(namespace, &keys)
    }

    #[pyo3(signature = (namespace, values, ttl_ms=None))]
    fn set_many(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        values: Py<PyDict>,
        ttl_ms: Option<u64>,
    ) -> PyResult<()> {
        let namespace = string_or_bytestring_as_string(namespace)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(argh)?;
        let expires_at = ttl_ms.map(|ttl| now + Duration::from_millis(ttl));
        let mut keys: Vec<String> = Vec::new();
        let mut python_values: Vec<DataAndMnemonic> = Vec::new();
        for (key, value) in values.bind(py).iter() {
            let key = key.extract::<StringOrByteString>()?;
            keys.push(string_or_bytestring_as_string(key)?);
            python_values.push(get_best_py_encoding(
                py,
                &value,
                self.settings.allow_pickle,
            )?);
        }
        py.allow_threads(move || {
            let mut dat_vec: Vec<DataAndMnemonics> = Vec::with_capacity(python_values.len());
            python_values
                .into_par_iter()
                .map(
                    |DataAndMnemonic {
                         data: py_enc_data,
                         codec: py_enc_mnemonic,
                     }| {
                        let data_enc_result = get_best_data_encoding(&py_enc_data).unwrap();
                        match data_enc_result {
                            Some(DataAndMnemonic {
                                data,
                                codec: mnemonic,
                            }) => DataAndMnemonics::from_two(data, py_enc_mnemonic, mnemonic),
                            None => DataAndMnemonics::from_data(py_enc_data, py_enc_mnemonic), // Didn't encode further
                        }
                    },
                )
                .collect_into_vec(&mut dat_vec);
            let mut iits: Vec<InternalInsertTriple> = Vec::with_capacity(keys.len());
            for (
                key,
                DataAndMnemonics {
                    data: value,
                    codecs: codecs_blob,
                },
            ) in keys.into_iter().zip(dat_vec)
            {
                iits.push(InternalInsertTriple {
                    key,
                    codecs_blob,
                    value,
                });
            }
            self.internal_insert(&namespace, now, expires_at, &iits)?;
            Ok(())
        })
    }

    #[pyo3(signature = (namespace, keys))]
    fn get_many(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        keys: Vec<StringOrByteString>,
    ) -> PyResult<PyObject> {
        let keys = string_or_bytestring_vector_as_string_vector(keys)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        let isrs = py.allow_threads(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| argh("Connection is closed"))?;
            let mut recs: Vec<InternalStoredRecord> = Vec::new();
            for keys in keys.chunks(self.max_num_binds) {
                let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query = format!(
                    "SELECT key, value, codecs, expires_at_ms FROM tl_{} WHERE key IN ({})",
                    namespace, placeholders
                );
                let mut stmt = conn.prepare(&query).map_err(argh)?;
                let chunk_recs = stmt
                    .query_map(rusqlite::params_from_iter(keys.iter()), |row| {
                        let codecs_blob = match row.get_ref(2)? {
                            ValueRef::Blob(v) => CodecsBlob::from_slice(v),
                            _ => panic!("invalid codec blob type"),
                        };
                        Ok(InternalStoredRecord {
                            key: Some(row.get(0)?),
                            value: row.get(1)?,
                            codecs_blob,
                            expires_at_ms: row.get(3)?,
                        })
                    })
                    .map_err(argh)?
                    .collect::<Result<Vec<InternalStoredRecord>, _>>()
                    .map_err(argh)?;
                recs.extend(chunk_recs);
            }
            recs.into_par_iter()
                .map(|isr| isr.into_data_codecs_decoded())
                .collect::<PyResult<Vec<InternalStoredDataAndMnemonic>>>()
                .map_err(argh)
        })?;
        let dict = PyDict::new(py);
        for isr in isrs {
            // TODO: check expiries
            let (key, py_val) = isr.into_python(py, &self.settings)?;
            dict.set_item(key.unwrap(), py_val)?;
        }
        Ok(dict.into())
    }

    #[pyo3(signature = (namespace, like=None))]
    fn list_keys(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        like: Option<StringOrByteString>,
    ) -> PyResult<Vec<String>> {
        let namespace = string_or_bytestring_as_string(namespace)?;
        let like = like.map(string_or_bytestring_as_string).transpose()?;
        py.allow_threads(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| argh("Connection is closed"))?;
            let query = match &like {
                Some(_like) => format!("SELECT key FROM tl_{} WHERE key LIKE ?", namespace),
                None => format!("SELECT key FROM tl_{}", namespace),
            };
            let mut stmt = conn.prepare(&query).map_err(argh)?;
            let keys = match like {
                Some(like) => stmt
                    .query_map(params![like], |row| row.get(0))
                    .map_err(argh)?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(argh)?,
                None => stmt
                    .query_map([], |row| row.get(0))
                    .map_err(argh)?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(argh)?,
            };
            Ok::<Vec<String>, PyErr>(keys)
        })
    }
}
