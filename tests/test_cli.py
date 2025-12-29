import contextlib
import io
import json
from collections import defaultdict

import pytest
import talsi


@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    """Create a test database with sample data."""
    db_path = tmp_path_factory.mktemp("talsi") / "test.db"
    with talsi.Storage(str(db_path), allow_pickle=False) as storage:
        # Add test data
        storage.set("users", "alice", {"name": "Alice", "age": 30})
        storage.set("users", "bob", "Bob's data")
        storage.set("config", "theme", "dark")
        storage.set("config", "lang", "en")
        storage.set("data", "image", b"\x89PNG\r\n\x1a\n")
        storage.set("data", "text", "Hello, world!")
    return str(db_path)


class CLIRunError(Exception):
    def __init__(self, error, stdout: str, stderr: str, status_code: int = 0):
        self.stdout = stdout
        self.stderr = stderr
        self.status_code = status_code
        super().__init__(error)


def run_cli(*args) -> str:
    from talsi.__main__ import main

    with (
        contextlib.redirect_stdout(io.StringIO()) as f,
        contextlib.redirect_stderr(io.StringIO()) as ferr,
    ):
        try:
            ret = main(list(args))
            if ret:
                raise ValueError(f"CLI exited with code {ret}")
        except SystemExit as se:
            raise CLIRunError(
                "CLI exited",
                f.getvalue().strip(),
                ferr.getvalue().strip(),
                se.code,
            ) from se
        except Exception as e:
            raise CLIRunError(str(e), f.getvalue().strip(), ferr.getvalue().strip()) from e

        return f.getvalue().strip()


def run_cli_binary(*args) -> bytes:
    from talsi.__main__ import main

    fb = io.BytesIO()
    with contextlib.redirect_stdout(io.StringIO()) as f:
        f.buffer = fb
        ret = main(list(args))
        if ret:
            raise ValueError(f"CLI exited with code {ret}")
        assert not f.getvalue(), "nothing should have been written to stdout text"
        return fb.getvalue()


def test_list_namespaces(test_db):
    """Test listing all namespaces."""
    result = run_cli("-f", test_db, "list-namespaces")
    assert set(result.split("\n")) == {"users", "config", "data"}


def test_list_keys_all_namespaces(test_db):
    """Test listing keys from all namespaces."""
    result = run_cli("-f", test_db, "list-keys")

    entries = defaultdict(set)
    for line in result.split("\n"):
        ns, key = line.split("\t")
        entries[ns].add(key)

    assert entries["users"] == {"alice", "bob"}
    assert entries["config"] == {"theme", "lang"}
    assert entries["data"] == {"image", "text"}


def test_list_keys_single_namespace(test_db):
    """Test listing keys from a specific namespace."""
    result = run_cli("-f", test_db, "list-keys", "-n", "users")
    assert set(result.split("\n")) == {"alice", "bob"}


def test_list_keys_empty_namespace(test_db):
    """Test listing keys from a non-existent namespace."""
    result = run_cli("-f", test_db, "list-keys", "-n", "nonexistent")
    assert result == ""


def test_get_single_key_json(test_db):
    """Test getting a single key with JSON value."""
    result = run_cli("-f", test_db, "get", "-n", "users", "-k", "alice")
    assert json.loads(result) == {"name": "Alice", "age": 30}


def test_get_single_key_string(test_db):
    """Test getting a single key with string value."""
    result = run_cli("-f", test_db, "get", "-n", "config", "-k", "theme")
    assert result == "dark"


def test_get_single_key_binary(test_db):
    """Test getting a single key with binary value."""
    result = run_cli_binary("-f", test_db, "get", "-n", "data", "-k", "image")
    assert result == b"\x89PNG\r\n\x1a\n"


def test_get_all_keys_in_namespace(test_db):
    """Test getting all keys in a namespace."""
    result = run_cli("-f", test_db, "get", "-n", "config")
    entries = dict(line.split("\t", 1) for line in result.split("\n"))
    assert entries == {"theme": "dark", "lang": "en"}


def test_get_nonexistent_key(test_db):
    """Test getting a non-existent key raises an error."""
    with pytest.raises(CLIRunError) as ei:
        run_cli("-f", test_db, "get", "-n", "users", "-k", "nonexistent")
    assert "not found" in ei.value.stderr
