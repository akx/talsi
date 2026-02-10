import threading

import pytest
import talsi

from .utils import (
    check_bk_dict_equal,
    get_values,
    measure_duration,
    n_test_keys,
    to_bytes,
)


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
