import sqlite3
from collections import Counter

import pytest
import talsi


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


def test_list_namespaces(tmp_path):
    """Test that namespace names with non-SQL-identifier characters work correctly."""
    db_path = str(tmp_path / "special_chars.db")

    # Test data
    namespaces = [
        "hige",
        "gallery-html-raw",
        "namespace@with@symbols",
        "   into spaaaaace    ",
        "delete",  # little Bobby Tables
    ]

    with talsi.Storage(db_path) as storage:
        for ns in namespaces:
            storage.set(ns, "test_key", "test_value")
        assert set(namespaces) == set(storage.list_namespaces())


def test_invalid_compression_algorithm(tmp_path):
    """Test that invalid compression algorithm raises an error."""
    with pytest.raises(talsi.TalsiError) as exc_info:
        talsi.Storage(str(tmp_path / "invalid.db"), compression="invalid")

    assert "Unknown compression algorithm" in str(exc_info.value)


def test_zstd_level_validation(tmp_path):
    """Test that invalid Zstd compression levels are rejected."""
    db_path = str(tmp_path / "invalid_level.db")

    # Test level too low
    with pytest.raises(talsi.TalsiError) as exc_info:
        talsi.Storage(db_path, compression="zstd:0")
    assert "must be between 1 and 22" in str(exc_info.value)

    # Test level too high
    with pytest.raises(talsi.TalsiError) as exc_info:
        talsi.Storage(db_path, compression="zstd:23")
    assert "must be between 1 and 22" in str(exc_info.value)

    # Test non-numeric level
    with pytest.raises(talsi.TalsiError) as exc_info:
        talsi.Storage(db_path, compression="zstd:abc")
    assert "Invalid zstd compression level" in str(exc_info.value)
