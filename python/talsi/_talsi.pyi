from typing import Any, AnyStr, Self

__all__ = ["Storage", "TalsiError", "setup_logging"]

class TalsiError(Exception): ...

def setup_logging() -> None: ...

class Storage:
    def __new__(
        cls,
        path: str,
        *,
        allow_pickle: bool = False,
        compression: str = "snappy",
    ) -> Self: ...
    def close(self) -> None: ...

    # Create/Update
    def set(
        self,
        namespace: AnyStr,
        key: AnyStr,
        value: Any,
        *,
        ttl_ms: int | None = None,
    ) -> None: ...
    def set_many(
        self,
        namespace: AnyStr,
        values: dict[AnyStr, Any],
        *,
        ttl_ms: int | None = None,
    ) -> int: ...

    # Retrieve
    def get(self, namespace: AnyStr, key: AnyStr) -> Any | None: ...
    def get_many(self, namespace: AnyStr, keys: list[AnyStr]) -> dict[str, Any]: ...
    def has(self, namespace: AnyStr, key: AnyStr) -> bool: ...
    def has_many(self, namespace: AnyStr, keys: list[AnyStr]) -> frozenset[str]: ...
    def list_keys(self, namespace: AnyStr, *, like: AnyStr | None = None) -> list[str]: ...
    def list_namespaces(self) -> list[str]: ...

    # Rename
    def rename(
        self,
        namespace: AnyStr,
        names: dict[AnyStr, AnyStr],
        *,
        overwrite: bool = False,
        must_exist: bool = True,
    ) -> int: ...

    # Delete
    def delete(self, namespace: AnyStr, key: AnyStr) -> int: ...
    def delete_many(self, namespace: AnyStr, keys: list[AnyStr]) -> int: ...
