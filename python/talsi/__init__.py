from collections.abc import Iterable
from functools import partial
from typing import Any, Self

from talsi._talsi import Storage as _Storage
from talsi._talsi import TalsiError, setup_logging
from talsi.helpers import batched

__all__ = [
    "Namespace",
    "Storage",
    "TalsiError",
    "setup_logging",
]


class Storage(_Storage):
    def get_many_batched(
        self,
        namespace: str | bytes,
        keys: Iterable[str],
        *,
        batch_size: int = 500,
    ) -> Iterable[tuple[str, Any]]:
        """
        Get many keys from a namespace in batches of up to `batch_size` items.

        More efficient than calling `get` in sequence, and more memory-efficient
        than calling `get_many` with all keys at once.

        :return: Iterable of key-value pairs.
        """
        for batch in batched(keys, batch_size):
            yield from self.get_many(namespace, batch).items()

    def items_batched(
        self,
        namespace: str | bytes,
        *,
        keys_like: str | None = None,
        batch_size: int = 500,
    ) -> Iterable[tuple[str, Any]]:
        """
        Get all key-value pairs in a namespace in batches of up to `batch_size` items.
        """
        keys = self.list_keys(namespace, like=keys_like)
        return self.get_many_batched(namespace, keys, batch_size=batch_size)

    def __enter__(self) -> Self:
        """
        Enter the context manager, returning the Storage instance.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the context manager, closing the Storage instance.
        """
        self.close()


class Namespace:
    """
    A dict-like view of a single namespace in a Storage.

    Note that since `Storage.get` returns `None` for missing keys,
    a stored `None` value is indistinguishable from a missing key,
    and `namespace[key]` will raise `KeyError` for it.
    """

    def __init__(self, talsi: _Storage, namespace: str | bytes):
        self._get = partial(talsi.get, namespace)
        self._set = partial(talsi.set, namespace)
        self._has = partial(talsi.has, namespace)
        self._delete = partial(talsi.delete, namespace)

    def __getitem__(self, key: str | bytes):
        value = self._get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __delitem__(self, key: str | bytes):
        if not self._delete(key):
            raise KeyError(key)

    def __setitem__(self, key: str | bytes, value: Any):
        self._set(key, value)

    def __contains__(self, key: str | bytes) -> bool:
        return self._has(key)
