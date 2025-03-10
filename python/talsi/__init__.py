from functools import partial
from typing import Any, Iterable

from talsi._talsi import Storage as _Storage
from talsi._talsi import TalsiError, setup_logging
from talsi.helpers import batched

__all__ = [
    "Storage",
    "TalsiError",
    "setup_logging",
    "Namespace",
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


class Namespace:
    def __init__(self, talsi: Storage, namespace: str | bytes):
        namespace = namespace.encode("utf-8")
        self._get = partial(talsi.get, namespace=namespace)
        self._set = partial(talsi.set, namespace=namespace)
        self._has = partial(talsi.has, namespace=namespace)

    def __getitem__(self, key: str | bytes):
        value = self._get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key: str | bytes, value: Any):
        self._set(key, value)

    def __contains__(self, key: str | bytes) -> bool:
        return self._has(key)
