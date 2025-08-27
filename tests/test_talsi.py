import contextlib
import sqlite3
import threading
import time
from collections import Counter

import pytest
import talsi

n_test_keys = 500


@contextlib.contextmanager
def measure_duration(name):
    start = time.time()
    yield
    end = time.time()
    time_taken_fmt = f"{end - start:.3f}"
    print(f"{name:35s} took {time_taken_fmt:7} seconds")


def to_bytes(key):
    if isinstance(key, bytes):
        return key
    return str(key).encode()


def get_values(key_bytes: bool, n: int):
    cast_key = to_bytes if key_bytes else str
    long_values = {cast_key(x): str(x).encode() * 10000 for x in range(n)}
    short_values = {
        cast_key(x): f"hello {x}" if x % 2 else {"structured": f"hello {x}"} for x in range(n)
    }
    return (long_values, short_values)


def dict_to_bytes_keys(d):
    return {to_bytes(key): value for key, value in d.items()}


def check_bk_dict_equal(d1, d2):
    return dict_to_bytes_keys(d1) == dict_to_bytes_keys(d2)


@pytest.fixture
def storage(request, tmp_path):
    if request.param == "pickle":
        return talsi.Storage(str(tmp_path / "pkl.db"), allow_pickle=True)
    if request.param == "json":
        return talsi.Storage(str(tmp_path / "json.db"), allow_pickle=False)
    raise ValueError(f"Unknown storage type: {request.param}")


@pytest.mark.parametrize("storage", ["pickle", "json"], indirect=True)
@pytest.mark.parametrize("key_bytes", [False, True], ids=["str", "bytes"])
@pytest.mark.parametrize("n", [n_test_keys])
def test_single(storage: talsi.Storage, key_bytes: bool, n: int):
    prefix = ""
    long_values, short_values = get_values(key_bytes, n)

    with measure_duration(f"{prefix} Single Set Short"):
        for key, value in short_values.items():
            storage.set("short_vals_single", key, value)

    with measure_duration(f"{prefix} Single Get Short"):
        for key, value in short_values.items():
            assert storage.get("short_vals_single", key) == value

    with measure_duration(f"{prefix} Single Has Short"):
        for key in short_values:
            assert storage.has("short_vals_single", key)

    with measure_duration(f"{prefix} Single Delete Short"):
        for key in short_values:
            assert storage.delete("short_vals_single", key)

    with measure_duration(f"{prefix} Single Set Long"):
        for key, value in long_values.items():
            storage.set("long_vals_single", key, value)

    with measure_duration(f"{prefix} Single Get Long"):
        assert check_bk_dict_equal(
            long_values,
            {key: storage.get("long_vals_single", key) for key in long_values},
        )

    with measure_duration(f"{prefix} List Keys"):
        s1 = {to_bytes(k) for k in storage.list_keys("long_vals_single")}
        s2 = {to_bytes(k) for k in long_values}
        assert s1 == s2


@pytest.mark.parametrize("storage", ["pickle", "json"], indirect=True)
@pytest.mark.parametrize("key_bytes", [False, True], ids=["str", "bytes"])
@pytest.mark.parametrize("n", [n_test_keys])
def test_many(storage: talsi.Storage, key_bytes: bool, n: int):
    prefix = ""
    long_values, short_values = get_values(key_bytes, n)

    with measure_duration(f"{prefix} Many Set Short"):
        storage.set_many("short_vals_many", short_values)

    with measure_duration(f"{prefix} Many Get Short"):
        assert check_bk_dict_equal(
            short_values,
            storage.get_many("short_vals_many", list(short_values)),
        )

    with measure_duration(f"{prefix} Many Has"):
        s1 = {to_bytes(k) for k in storage.has_many("short_vals_many", list(short_values))}
        s2 = {to_bytes(k) for k in short_values}
        assert s1 == s2

    with measure_duration(f"{prefix} Many Set Long"):
        storage.set_many("long_vals_many", long_values)

    with measure_duration(f"{prefix} Many Get Long"):
        assert check_bk_dict_equal(
            long_values,
            storage.get_many("long_vals_many", list(long_values)),
        )

    with measure_duration(f"{prefix} Many Delete Short"):
        assert storage.delete_many("short_vals_many", list(short_values)) == len(
            short_values,
        )


def threading_inner(storage: talsi.Storage, i: int, n: int):
    for x in range(n):
        storage.set(f"ns_{i}", str(x), f"hello {x}")


@pytest.mark.parametrize("storage", ["pickle", "json"], indirect=True)
def test_threading(storage: talsi.Storage):
    n_threads = 10
    threads = [
        threading.Thread(target=threading_inner, args=(storage, i, n_test_keys))
        for i in range(n_threads)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for x in range(n_threads):
        assert len(storage.list_keys(f"ns_{x}")) == n_test_keys
    storage.close()


def test_sqlite3_interop(tmp_path):
    db_path = str(tmp_path / "interop.db")
    storage = talsi.Storage(db_path)
    storage.set_many("ns", {"key": "value", "avain": 8})
    storage.close()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT key, value FROM tl_ns")
        # In reality, you can't read data out of a Talsi database like this;
        # you'd have to look at the codec chain too.
        assert dict(cursor.fetchall()) == {"avain": b"8", "key": b"value"}


def test_get_from_empty(tmp_path):
    db_path = str(tmp_path / "empty.db")
    with talsi.Storage(db_path) as storage:
        assert storage.get("foo", "bar") is None
        assert storage.has("foo", "bar") is False
        assert storage.has_many("foo", ["bar", "baz"]) == frozenset()
        assert storage.get_many("foo", ["bar", "baz"]) == {}
        assert storage.list_keys("foo") == []


def test_irregular_types(tmp_path):
    db_path = str(tmp_path / "empty.db")
    c = Counter(["bar", "bar", "baz", "baz", "baz"])
    with talsi.Storage(db_path) as storage:
        assert storage.set_many("foo", c) == len(c) == 2
        # TODO: support a generator here instead
        assert storage.get_many("foo", ["b" + suffix for suffix in ("ar", "az")]) == {
            "bar": 2,
            "baz": 3,
        }


def test_namespace_names_with_special_characters(tmp_path):
    """Test that namespace names with non-SQL-identifier characters work correctly."""
    db_path = str(tmp_path / "special_chars.db")

    # Test data
    values = [
        "hige",
        {"test_key": "test_value", "another_key": 42},
        "aunt",
    ]

    with talsi.Storage(db_path) as storage:
        problematic_namespaces = [
            "gallery-html-raw",  # Issue #5
            "namespace-with-hyphens",
            "namespace with spaces",
            "namespace.with.dots",
            "namespace@with@symbols",
            "namespace/with/slashes",
            "namespace\\with\\backslashes",
            "namespace(with)parens",
            "namespace[with]brackets",
            "namespace{with}braces",
            "namespace:with:colons",
            "namespace;with;semicolons",
            "namespace,with,commas",
            "namespace'with'quotes",
            "namespace`with`backticks",
            "namespace|with|pipes",
            "namespace<with>angles",
            "namespace+with+plus",
            "namespace=with=equals",
            "namespace%with%percent",
            "namespace&with&ampersand",
            "namespace*with*stars",
            "namespace#with#hash",
            "namespace!with!exclamation",
            "namespace?with?question",
            "namespace~with~tilde",
            "namespace^with^caret",
            "123numeric_start",  # starts with number
            "---...@@@",
            "namespace_with_ünïcödé_characters",
            'double"quote',
            "",
        ]

        # Test SQL reserved words as namespaces
        sql_keywords = [
            "select",
            "insert",
            "update",
            "delete",
            "create",
            "drop",
            "alter",
            "table",
            "index",
            "view",
            "database",
            "schema",
            "from",
            "where",
            "group",
            "order",
            "having",
            "limit",
            "offset",
            "union",
            "join",
            "primary",
            "key",
            "foreign",
            "references",
            "unique",
            "constraint",
        ]

        # Test each problematic namespace
        for namespace in problematic_namespaces + sql_keywords:
            # Test basic operations
            storage.set(namespace, "test_key", "test_value")
            assert storage.get(namespace, "test_key") == "test_value"
            assert storage.has(namespace, "test_key") is True

            # Test batch operations
            batch_data = {f"key_{i}": value for i, value in enumerate(values)}
            storage.set_many(namespace, batch_data)
            retrieved_batch = storage.get_many(namespace, list(batch_data.keys()))
            assert retrieved_batch == batch_data

            # Test list_keys
            keys = storage.list_keys(namespace)
            expected_keys = {"test_key"} | set(batch_data.keys())
            assert set(keys) == expected_keys

            # Test has_many
            has_result = storage.has_many(namespace, list(expected_keys))
            assert has_result == expected_keys

            # Clean up for next test
            storage.delete_many(namespace, list(expected_keys))
