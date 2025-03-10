from itertools import islice
from typing import Any, Iterable


def batched(iterable: Iterable[Any], batch_size: int) -> Iterable[list[Any]]:
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        yield batch
