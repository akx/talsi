import pytest
import talsi

from .utils import storage_types


@pytest.fixture(params=storage_types)
def storage(request, tmp_path):
    type, _, compression = request.param.partition(":")
    if type == "pickle":
        return talsi.Storage(str(tmp_path / "pkl.db"), allow_pickle=True, compression=compression)
    if type == "json":
        return talsi.Storage(str(tmp_path / "json.db"), allow_pickle=False, compression=compression)
    raise ValueError(f"Unknown storage type: {request.param}")
