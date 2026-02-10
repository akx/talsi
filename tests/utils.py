import contextlib
import time

storage_types = ["pickle:snappy", "json:snappy", "pickle:zstd", "json:zstd"]
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
