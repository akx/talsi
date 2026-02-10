import pytest
import talsi


def test_rename_basic(tmp_path):
    """Test basic rename functionality."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        storage.set_many("ns", {"a": 1, "b": 2, "c": 3})
        n = storage.rename("ns", {"a": "x", "b": "y"})
        assert n == 2
        assert storage.get("ns", "x") == 1
        assert storage.get("ns", "y") == 2
        assert storage.get("ns", "a") is None
        assert storage.get("ns", "b") is None
        assert storage.get("ns", "c") == 3  # untouched


def test_rename_noop_same_name(tmp_path):
    """Test that renaming a key to itself is a no-op counted as success."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        storage.set("ns", "a", 1)
        n = storage.rename("ns", {"a": "a"})
        assert n == 1
        assert storage.get("ns", "a") == 1


def test_rename_must_exist_true(tmp_path):
    """Test that must_exist=True raises when the old key is missing."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        storage.set("ns", "a", 1)
        with pytest.raises(talsi.TalsiError, match="does not exist"):
            storage.rename("ns", {"missing": "x"})
        # Original key untouched (transaction rolled back)
        assert storage.get("ns", "a") == 1


def test_rename_must_exist_false(tmp_path):
    """Test that must_exist=False skips missing keys silently."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        storage.set("ns", "a", 1)
        n = storage.rename("ns", {"a": "x", "missing": "y"}, must_exist=False)
        assert n == 1
        assert storage.get("ns", "x") == 1
        assert storage.get("ns", "y") is None


def test_rename_overwrite_false(tmp_path):
    """Test that overwrite=False raises when the target key exists."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        storage.set_many("ns", {"a": 1, "b": 2})
        with pytest.raises(talsi.TalsiError, match="already exists"):
            storage.rename("ns", {"a": "b"})
        # Both keys untouched (transaction rolled back)
        assert storage.get("ns", "a") == 1
        assert storage.get("ns", "b") == 2


def test_rename_overwrite_true(tmp_path):
    """Test that overwrite=True replaces the target key."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        storage.set_many("ns", {"a": 1, "b": 2})
        n = storage.rename("ns", {"a": "b"}, overwrite=True)
        assert n == 1
        assert storage.get("ns", "a") is None
        assert storage.get("ns", "b") == 1


def test_rename_empty_namespace(tmp_path):
    """Test rename on a non-existent namespace."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        # must_exist=True (default) with missing namespace
        with pytest.raises(talsi.TalsiError, match="does not exist"):
            storage.rename("nonexistent", {"a": "b"})
        # must_exist=False with missing namespace
        n = storage.rename("nonexistent", {"a": "b"}, must_exist=False)
        assert n == 0


def test_rename_empty_dict(tmp_path):
    """Test rename with empty dict is a no-op."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        n = storage.rename("ns", {})
        assert n == 0


def test_rename_bytes_keys(tmp_path):
    """Test rename with bytes keys."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path) as storage:
        storage.set("ns", b"a", 1)
        n = storage.rename("ns", {b"a": b"x"})
        assert n == 1
        assert storage.get("ns", "x") == 1
        assert storage.get("ns", "a") is None


def test_rename_preserves_value_and_metadata(tmp_path):
    """Test that rename preserves the stored value including codecs."""
    db_path = str(tmp_path / "rename.db")
    with talsi.Storage(db_path, allow_pickle=True) as storage:
        original_value = {"nested": [1, 2, 3], "key": "value"}
        storage.set("ns", "old", original_value)
        storage.rename("ns", {"old": "new"})
        assert storage.get("ns", "new") == original_value

    # Also test with a large value (triggers compression)
    with talsi.Storage(db_path) as storage:
        large_value = "x" * 10000
        storage.set("ns2", "old", large_value)
        storage.rename("ns2", {"old": "new"})
        assert storage.get("ns2", "new") == large_value
