import pytest
import talsi


@pytest.mark.parametrize("ns_name", ["ns", b"ns"])
def test_namespace(storage, ns_name):
    ns = talsi.Namespace(storage, ns_name)
    ns["a"] = {"hello": "world"}
    ns[b"b"] = 42
    assert ns["a"] == {"hello": "world"}
    assert ns["b"] == 42
    assert "a" in ns
    assert b"b" in ns
    assert "missing" not in ns
    with pytest.raises(KeyError):
        ns["missing"]
    # Visible through the underlying storage too
    assert storage.get("ns", "a") == {"hello": "world"}
    del ns["a"]
    assert "a" not in ns
    with pytest.raises(KeyError):
        del ns["a"]


def test_namespace_isolation(storage):
    ns1 = talsi.Namespace(storage, "one")
    ns2 = talsi.Namespace(storage, "two")
    ns1["k"] = 1
    ns2["k"] = 2
    assert ns1["k"] == 1
    assert ns2["k"] == 2
    del ns1["k"]
    assert "k" not in ns1
    assert ns2["k"] == 2
