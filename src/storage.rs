use crate::data_codecs::{
    CompressionAlgorithm, decode_from_data_and_mnemonic, get_best_data_encoding,
};
use crate::py_codecs::{decode_to_python_from_data_and_mnemonic, get_best_py_encoding};
use crate::typ::{CodecsBlob, DataAndMnemonic, DataAndMnemonics, StringOrByteString};
use crate::utils;
use either::Either;
use eyre::Context;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFrozenSet};
use pyo3::{Bound, Py, PyAny, PyErr, PyResult, Python, pyclass, pymethods};
use rayon::prelude::*;
use rusqlite::limits::Limit;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, params};
use rusqlite::{OptionalExtension, params_from_iter};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::instrument;
use utils::to_talsi_error;

type CowStr = Cow<'static, str>;

lazy_static::lazy_static! {
    static ref QUOTED_TABLE_NAME_CACHE: RwLock<HashMap<CowStr, CowStr>> = RwLock::new(HashMap::new());
}

#[inline]
fn get_quoted_table_name(namespace: &str) -> CowStr {
    if let Ok(cache) = QUOTED_TABLE_NAME_CACHE.read()
        && let Some(cached) = cache.get(namespace)
    {
        return cached.clone();
    }

    let quoted = format!("\"tl_{}\"", namespace.replace('"', "\"\""));
    let cow_quoted = Cow::from(quoted);

    // Try to cache, but don't fail if we can't
    let _ = QUOTED_TABLE_NAME_CACHE
        .write()
        .map(|mut cache| cache.insert(Cow::from(namespace.to_owned()), cow_quoted.clone()));

    cow_quoted
}

fn get_quoted_key_index_name(namespace: &str) -> String {
    format!("\"tl_{}_key\"", namespace.replace('"', "\"\""))
}

fn string_or_bytestring_as_string(sobs: StringOrByteString) -> eyre::Result<CowStr> {
    match sobs {
        Either::Left(s) => Ok(Cow::from(s)),
        Either::Right(b) => {
            let s = std::str::from_utf8(&b).wrap_err("bytestring didn't parse as utf-8")?;
            Ok(Cow::from(s.to_owned()))
        }
    }
}

fn strings_or_bytestrings_as_strings(sobses: Vec<StringOrByteString>) -> eyre::Result<Vec<CowStr>> {
    let mut res = Vec::with_capacity(sobses.len());
    for sobs in sobses {
        res.push(string_or_bytestring_as_string(sobs)?);
    }
    Ok(res)
}

struct StorageSettings {
    pub allow_pickle: bool,
    pub compression: CompressionAlgorithm,
}

#[pyclass(subclass, module = "talsi._talsi")]
pub struct Storage {
    conn: Mutex<Option<Connection>>,
    known_namespaces: RwLock<HashSet<CowStr>>,
    settings: StorageSettings,
    max_num_binds: usize,
}

struct InternalInsertTriple {
    key: CowStr,
    codecs_blob: CodecsBlob,
    value: Vec<u8>,
}

struct InternalStoredRecord {
    key: Option<CowStr>, // we may have not queried this
    codecs_blob: CodecsBlob,
    value: Vec<u8>,
    expires_at_ms: Option<i64>,
}
struct InternalStoredDataAndMnemonic {
    key: Option<CowStr>, // we may have not queried this
    #[allow(dead_code)]
    expires_at_ms: Option<i64>,
    data_and_mnemonic: DataAndMnemonic,
}

impl InternalStoredDataAndMnemonic {
    fn into_python<'py>(
        self,
        py: Python<'py>,
        s: &StorageSettings,
    ) -> PyResult<(Option<CowStr>, Bound<'py, PyAny>)> {
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
            .ok_or_else(|| to_talsi_error("No codec mnemonic found"))?;
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

fn ensure_namespace_table(conn: &Connection, namespace: &str) -> PyResult<()> {
    let table_name = get_quoted_table_name(namespace);
    let quoted_index_name = get_quoted_key_index_name(namespace);
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                    key TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 0,
                    codecs BLOB NOT NULL,
                    value BLOB NOT NULL,
                    created_at_ms TIMESTAMP NOT NULL,
                    expires_at_ms TIMESTAMP,
                    PRIMARY KEY (key, version)
                )",
        ),
        [],
    )
    .map_err(to_talsi_error)?;
    conn.execute(
        &format!("CREATE INDEX IF NOT EXISTS {quoted_index_name} ON {table_name} (key)"),
        [],
    )
    .map_err(to_talsi_error)?;
    Ok(())
}

enum StatementResult<S> {
    Stmt(S),
    NoSuchTable,
}

fn ignore_no_such_table<S>(
    r: Result<S, rusqlite::Error>,
) -> Result<StatementResult<S>, rusqlite::Error> {
    match r {
        Ok(stmt) => Ok(StatementResult::Stmt(stmt)),
        Err(e) => match e {
            rusqlite::Error::SqliteFailure(_, Some(ref reason_string))
                if reason_string.starts_with("no such table:") =>
            {
                Ok(StatementResult::NoSuchTable)
            }
            _ => Err(e),
        },
    }
}

impl Storage {
    fn ensure_namespace_table(&self, conn_lock: &Connection, namespace: &str) -> PyResult<()> {
        let known_namespaces = self.known_namespaces.read().unwrap();
        // If we've already created the table, don't do it again.
        if known_namespaces.contains(namespace) {
            return Ok(());
        }
        drop(known_namespaces);
        let mut known_namespaces = self.known_namespaces.write().unwrap();
        ensure_namespace_table(conn_lock, namespace)?;
        known_namespaces.insert(Cow::from(namespace.to_owned()));
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
    ) -> PyResult<usize> {
        let table_name = get_quoted_table_name(namespace);
        let now_ms = now.as_millis() as i64;
        let expires_ms = expires_at.map(|t| t.as_millis() as i64);
        let maybe_conn = self.conn.lock().unwrap();
        let conn = maybe_conn
            .as_ref()
            .ok_or_else(|| to_talsi_error("Connection is closed"))?;
        self.ensure_namespace_table(conn, namespace)?;
        let tx = conn.unchecked_transaction().map_err(to_talsi_error)?;
        let mut stmt = tx
            .prepare_cached(&format!("INSERT OR REPLACE INTO {table_name} (key, codecs, value, created_at_ms, expires_at_ms) VALUES (?, ?, ?, ?, ?)"))
            .map_err(to_talsi_error)?;
        for iit in iits {
            let InternalInsertTriple {
                key,
                codecs_blob,
                value: data_encoded,
            } = iit;
            stmt.execute(params![
                key.as_ref(),
                codecs_blob.as_slice(),
                data_encoded,
                now_ms,
                expires_ms
            ])
            .map_err(to_talsi_error)?;
        }
        drop(stmt);
        tx.commit().map_err(to_talsi_error)?;
        Ok(iits.len())
    }

    #[inline]
    #[instrument(skip_all)]
    fn internal_delete(&self, namespace: CowStr, keys: &[CowStr]) -> PyResult<usize> {
        let maybe_conn = self.conn.lock().unwrap();
        let conn = maybe_conn
            .as_ref()
            .ok_or_else(|| to_talsi_error("Connection is closed"))?;
        let tx = conn.unchecked_transaction().map_err(to_talsi_error)?;
        let mut n = 0;
        let table_name = get_quoted_table_name(&namespace);
        for keys in keys.chunks(self.max_num_binds) {
            let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = &format!("DELETE FROM {table_name} WHERE key IN ({placeholders})");
            let mut stmt = match ignore_no_such_table(tx.prepare(query)).map_err(to_talsi_error)? {
                StatementResult::Stmt(stmt) => stmt,
                StatementResult::NoSuchTable => {
                    return Ok(0);
                }
            };
            let res = stmt.execute(params_from_iter(keys.iter().map(AsRef::as_ref)));
            match res {
                Ok(rows) => {
                    n += rows;
                }
                Err(e) => {
                    if e.to_string().contains("no such table") {
                        return Ok(0);
                    } else {
                        return Err(to_talsi_error(e));
                    }
                }
            }
        }
        tx.commit().map_err(to_talsi_error)?;
        Ok(n)
    }
}

const INIT_PRAGMAS: &str = "
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=1000;
PRAGMA temp_store=MEMORY;
";

#[pymethods]
impl Storage {
    #[new]
    #[pyo3(signature = (path, *, allow_pickle = false, compression = "snappy"))]
    fn new(path: &str, allow_pickle: bool, compression: &str) -> PyResult<Self> {
        let compression_algorithm = CompressionAlgorithm::from_str(compression)?;
        let conn = Connection::open(path).map_err(to_talsi_error)?;
        conn.set_prepared_statement_cache_capacity(64);
        conn.execute_batch(INIT_PRAGMAS).map_err(to_talsi_error)?;
        let max_num_binds = conn
            .limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
            .map_err(to_talsi_error)? as usize;

        Ok(Storage {
            conn: Mutex::new(Some(conn)),
            max_num_binds,
            known_namespaces: RwLock::new(HashSet::new()),
            settings: StorageSettings {
                allow_pickle,
                compression: compression_algorithm,
            },
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

    #[pyo3(signature = (namespace, key, value, *, ttl_ms=None))]
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
        py.detach(|| {
            let key = string_or_bytestring_as_string(key)?;
            let namespace = string_or_bytestring_as_string(namespace)?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(to_talsi_error)?;
            let expires_at = ttl_ms.map(|ttl| now + Duration::from_millis(ttl));
            let data_enc_result =
                get_best_data_encoding(&py_enc_result.data, self.settings.compression)?;
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
            self.internal_insert(namespace.as_ref(), now, expires_at, &[iit])?;
            Ok(())
        })
    }

    #[pyo3(signature = (namespace, key))]
    fn get(
        &mut self,
        py: Python<'_>,
        namespace: StringOrByteString,
        key: StringOrByteString,
    ) -> PyResult<Option<Py<PyAny>>> {
        let idd = py.detach(|| -> PyResult<Option<InternalStoredDataAndMnemonic>> {
            let key = string_or_bytestring_as_string(key)?;
            let namespace = string_or_bytestring_as_string(namespace)?;
            let table_name = get_quoted_table_name(&namespace);
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or(to_talsi_error("Connection is closed"))?;
            let mut stmt = match ignore_no_such_table(conn.prepare_cached(&format!(
                "SELECT value, codecs, expires_at_ms FROM {table_name} WHERE key = ? LIMIT 1",
            )))
            .map_err(to_talsi_error)?
            {
                StatementResult::Stmt(stmt) => stmt,
                StatementResult::NoSuchTable => {
                    return Ok(None);
                }
            };
            let isr: Option<InternalStoredRecord> = stmt
                .query_row(params![key.as_ref()], |row| {
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
                .map_err(to_talsi_error)?;
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

    #[pyo3(signature = (namespace, key))]
    fn has(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        key: StringOrByteString,
    ) -> PyResult<bool> {
        let key = string_or_bytestring_as_string(key)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        let table_name = get_quoted_table_name(&namespace);
        py.detach(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| to_talsi_error("Connection is closed"))?;
            let mut stmt = match ignore_no_such_table(conn.prepare_cached(&format!(
                "SELECT EXISTS(SELECT 1 FROM {table_name} WHERE key = ? LIMIT 1)",
            )))
            .map_err(to_talsi_error)?
            {
                StatementResult::Stmt(stmt) => stmt,
                StatementResult::NoSuchTable => {
                    return Ok(false);
                }
            };
            let exists: i64 = stmt
                .query_row(params![key.as_ref()], |row| row.get(0))
                .optional()
                .map_err(to_talsi_error)?
                .unwrap_or(0);
            Ok::<bool, PyErr>(exists != 0)
        })
    }

    #[pyo3(signature = (namespace, keys))]
    fn has_many(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        keys: Vec<StringOrByteString>,
    ) -> PyResult<Py<PyFrozenSet>> {
        let keys = strings_or_bytestrings_as_strings(keys)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        let table_name = get_quoted_table_name(&namespace);
        let extant_keys = py.detach(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| to_talsi_error("Connection is closed"))?;
            let mut extant_keys: HashSet<String> = HashSet::with_capacity(keys.len());
            for keys in keys.chunks(self.max_num_binds) {
                let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query = format!("SELECT key FROM {table_name} WHERE key IN ({placeholders})");
                let mut stmt =
                    match ignore_no_such_table(conn.prepare(&query)).map_err(to_talsi_error)? {
                        StatementResult::Stmt(stmt) => stmt,
                        StatementResult::NoSuchTable => {
                            return Ok::<HashSet<String>, PyErr>(extant_keys);
                        }
                    };
                let keys = stmt
                    .query_map(params_from_iter(keys.iter().map(AsRef::as_ref)), |row| {
                        row.get(0)
                    })
                    .map_err(to_talsi_error)?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(to_talsi_error)?;
                extant_keys.extend(keys);
            }
            Ok::<HashSet<String>, PyErr>(extant_keys)
        })?;
        let fz = PyFrozenSet::new(py, extant_keys)?;
        Ok(fz.into())
    }

    #[pyo3(signature = (namespace, key))]
    fn delete(&self, namespace: StringOrByteString, key: StringOrByteString) -> PyResult<usize> {
        let key = string_or_bytestring_as_string(key)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        self.internal_delete(namespace, &[key])
    }

    #[pyo3(signature = (namespace, keys))]
    fn delete_many(
        &self,
        namespace: StringOrByteString,
        keys: Vec<StringOrByteString>,
    ) -> PyResult<usize> {
        let keys = strings_or_bytestrings_as_strings(keys)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        self.internal_delete(namespace, &keys)
    }

    #[pyo3(signature = (namespace, values, *, ttl_ms=None))]
    fn set_many(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        values: Py<PyDict>,
        ttl_ms: Option<u64>,
    ) -> PyResult<usize> {
        let namespace = string_or_bytestring_as_string(namespace)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(to_talsi_error)?;
        let expires_at = ttl_ms.map(|ttl| now + Duration::from_millis(ttl));
        let mut keys: Vec<CowStr> = Vec::new();
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
        let compression = self.settings.compression;
        py.detach(move || {
            let mut dat_vec: Vec<DataAndMnemonics> = Vec::with_capacity(python_values.len());
            python_values
                .into_par_iter()
                .map(
                    |DataAndMnemonic {
                         data: py_enc_data,
                         codec: py_enc_mnemonic,
                     }| {
                        let data_enc_result =
                            get_best_data_encoding(&py_enc_data, compression).unwrap();
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
            self.internal_insert(namespace.as_ref(), now, expires_at, &iits)
        })
    }

    #[pyo3(signature = (namespace, keys))]
    fn get_many(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        keys: Vec<StringOrByteString>,
    ) -> PyResult<Py<PyAny>> {
        let keys = strings_or_bytestrings_as_strings(keys)?;
        let namespace = string_or_bytestring_as_string(namespace)?;
        let table_name = get_quoted_table_name(&namespace);
        let isrs = py.detach(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| to_talsi_error("Connection is closed"))?;
            let mut recs: Vec<InternalStoredRecord> = Vec::new();
            for keys in keys.chunks(self.max_num_binds) {
                let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query = format!(
                    "SELECT key, value, codecs, expires_at_ms FROM {table_name} WHERE key IN ({placeholders})",
                );
                let mut stmt =
                    match ignore_no_such_table(conn.prepare(&query)).map_err(to_talsi_error)? {
                        StatementResult::Stmt(stmt) => stmt,
                        StatementResult::NoSuchTable => {
                            break;
                        }
                    };
                let chunk_recs = stmt
                    .query_map(
                        rusqlite::params_from_iter(keys.iter().map(AsRef::as_ref)),
                        |row| {
                            let codecs_blob = match row.get_ref(2)? {
                                ValueRef::Blob(v) => CodecsBlob::from_slice(v),
                                _ => panic!("invalid codec blob type"),
                            };
                            let key: String = row.get(0)?;
                            Ok(InternalStoredRecord {
                                key: Some(Cow::from(key)),
                                value: row.get(1)?,
                                codecs_blob,
                                expires_at_ms: row.get(3)?,
                            })
                        },
                    )
                    .map_err(to_talsi_error)?
                    .collect::<Result<Vec<InternalStoredRecord>, _>>()
                    .map_err(to_talsi_error)?;
                recs.extend(chunk_recs);
            }
            recs.into_par_iter()
                .map(|isr| isr.into_data_codecs_decoded())
                .collect::<PyResult<Vec<InternalStoredDataAndMnemonic>>>()
                .map_err(to_talsi_error)
        })?;
        let dict = PyDict::new(py);
        for isr in isrs {
            // TODO: check expiries
            let (key, py_val) = isr.into_python(py, &self.settings)?;
            dict.set_item(key.unwrap().as_ref(), py_val)?;
        }
        Ok(dict.into())
    }

    #[pyo3(signature = (namespace, *, like=None))]
    fn list_keys(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        like: Option<StringOrByteString>,
    ) -> PyResult<Vec<String>> {
        let namespace = string_or_bytestring_as_string(namespace)?;
        let table_name = get_quoted_table_name(&namespace);
        let like = like.map(string_or_bytestring_as_string).transpose()?;
        py.detach(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| to_talsi_error("Connection is closed"))?;
            let query = match &like {
                Some(_like) => format!("SELECT key FROM {table_name} WHERE key LIKE ?"),
                None => format!("SELECT key FROM {table_name}"),
            };
            let mut stmt =
                match ignore_no_such_table(conn.prepare(&query)).map_err(to_talsi_error)? {
                    StatementResult::Stmt(stmt) => stmt,
                    StatementResult::NoSuchTable => {
                        return Ok::<Vec<String>, PyErr>(Vec::new());
                    }
                };
            let keys = match like {
                Some(like) => stmt
                    .query_map(params![like.as_ref()], |row| row.get(0))
                    .map_err(to_talsi_error)?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(to_talsi_error)?,
                None => stmt
                    .query_map([], |row| row.get(0))
                    .map_err(to_talsi_error)?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(to_talsi_error)?,
            };
            Ok::<Vec<String>, PyErr>(keys)
        })
    }

    #[pyo3(signature = (namespace, names, *, overwrite=false, must_exist=true))]
    fn rename(
        &self,
        py: Python<'_>,
        namespace: StringOrByteString,
        names: Py<PyDict>,
        overwrite: bool,
        must_exist: bool,
    ) -> PyResult<usize> {
        let mut all_pairs: Vec<(CowStr, CowStr)> = Vec::new();
        for (old_key, new_key) in names.bind(py).iter() {
            let old_key = old_key.extract::<StringOrByteString>()?;
            let new_key = new_key.extract::<StringOrByteString>()?;
            all_pairs.push((
                string_or_bytestring_as_string(old_key)?,
                string_or_bytestring_as_string(new_key)?,
            ));
        }
        let namespace = string_or_bytestring_as_string(namespace)?;

        // Separate no-op pairs (old == new) from real renames
        let noop_count = all_pairs.iter().filter(|(o, n)| o == n).count();
        let pairs: Vec<_> = all_pairs.into_iter().filter(|(o, n)| o != n).collect();

        if pairs.is_empty() {
            return Ok(noop_count);
        }

        let table_name = get_quoted_table_name(&namespace);
        let maybe_conn = self.conn.lock().unwrap();
        let conn = maybe_conn
            .as_ref()
            .ok_or_else(|| to_talsi_error("Connection is closed"))?;
        let tx = conn.unchecked_transaction().map_err(to_talsi_error)?;

        // Check if table exists
        {
            let check_query = format!("SELECT 1 FROM {table_name} LIMIT 0");
            match ignore_no_such_table(tx.prepare(&check_query)).map_err(to_talsi_error)? {
                StatementResult::Stmt(_) => {}
                StatementResult::NoSuchTable => {
                    if must_exist {
                        return Err(to_talsi_error(format!(
                            "Key '{}' does not exist",
                            pairs[0].0
                        )));
                    }
                    return Ok(noop_count);
                }
            }
        };

        let old_names_set: HashSet<String> =
            pairs.iter().map(|(o, _)| o.as_ref().to_owned()).collect();

        // Find which old keys actually exist
        let old_names_vec: Vec<&str> = pairs.iter().map(|(o, _)| o.as_ref()).collect();
        let mut existing_old_keys: HashSet<String> = HashSet::new();
        for chunk in old_names_vec.chunks(self.max_num_binds) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = format!("SELECT key FROM {table_name} WHERE key IN ({placeholders})");
            let mut stmt = tx.prepare(&query).map_err(to_talsi_error)?;
            let keys = stmt
                .query_map(params_from_iter(chunk.iter()), |row| {
                    row.get::<_, String>(0)
                })
                .map_err(to_talsi_error)?
                .collect::<Result<Vec<String>, _>>()
                .map_err(to_talsi_error)?;
            existing_old_keys.extend(keys);
        }

        if must_exist {
            for old_name in &old_names_vec {
                if !existing_old_keys.contains(*old_name) {
                    return Err(to_talsi_error(format!("Key '{}' does not exist", old_name)));
                }
            }
        }

        // Keep only pairs where the old key exists
        let pairs: Vec<_> = pairs
            .into_iter()
            .filter(|(o, _)| existing_old_keys.contains(o.as_ref()))
            .collect();

        if pairs.is_empty() {
            tx.commit().map_err(to_talsi_error)?;
            return Ok(noop_count);
        }

        // Collect new names that aren't also old names (those get renamed away atomically)
        let new_names_to_check: Vec<&str> = pairs
            .iter()
            .map(|(_, n)| n.as_ref())
            .filter(|n| !old_names_set.contains(*n))
            .collect();

        if !new_names_to_check.is_empty() {
            if !overwrite {
                // Check that no target keys already exist
                for chunk in new_names_to_check.chunks(self.max_num_binds) {
                    let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    let query = format!(
                        "SELECT key FROM {table_name} WHERE key IN ({placeholders}) LIMIT 1"
                    );
                    let mut stmt = tx.prepare(&query).map_err(to_talsi_error)?;
                    let existing: Option<String> = stmt
                        .query_row(params_from_iter(chunk.iter()), |row| row.get(0))
                        .optional()
                        .map_err(to_talsi_error)?;
                    if let Some(key) = existing {
                        return Err(to_talsi_error(format!("Key '{}' already exists", key)));
                    }
                }
            } else {
                // Delete conflicting target keys
                for chunk in new_names_to_check.chunks(self.max_num_binds) {
                    let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    let query = format!("DELETE FROM {table_name} WHERE key IN ({placeholders})");
                    tx.execute(&query, params_from_iter(chunk.iter()))
                        .map_err(to_talsi_error)?;
                }
            }
        }

        // Bulk UPDATE with CASE — 3 bind params per pair (2 in CASE + 1 in WHERE IN)
        let chunk_size = self.max_num_binds / 3;
        let mut n_renamed = 0usize;
        for chunk in pairs.chunks(chunk_size) {
            let case_clauses: String = chunk
                .iter()
                .map(|_| "WHEN ? THEN ?")
                .collect::<Vec<_>>()
                .join(" ");
            let where_placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = format!(
                "UPDATE {table_name} SET key = CASE key {case_clauses} END \
                 WHERE key IN ({where_placeholders})"
            );

            let mut param_values: Vec<&str> = Vec::with_capacity(chunk.len() * 3);
            for (old_name, new_name) in chunk {
                param_values.push(old_name.as_ref());
                param_values.push(new_name.as_ref());
            }
            for (old_name, _) in chunk {
                param_values.push(old_name.as_ref());
            }

            let rows = tx
                .execute(&query, params_from_iter(param_values.iter()))
                .map_err(to_talsi_error)?;
            n_renamed += rows;
        }

        tx.commit().map_err(to_talsi_error)?;
        Ok(n_renamed + noop_count)
    }

    fn list_namespaces(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| {
            let maybe_conn = self.conn.lock().unwrap();
            let conn = maybe_conn
                .as_ref()
                .ok_or_else(|| to_talsi_error("Connection is closed"))?;
            let mut stmt = conn
                .prepare("SELECT SUBSTR(name, 4) FROM sqlite_master WHERE type='table' AND name LIKE 'tl_%'")
                .map_err(to_talsi_error)?;
            let namespaces = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(to_talsi_error)?
                .filter_map(|name_result| name_result.ok())
                .collect::<Vec<String>>();
            Ok::<Vec<String>, PyErr>(namespaces)
        })
    }
}
