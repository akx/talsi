from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Callable

import talsi

if TYPE_CHECKING:
    import diskcache

log = logging.getLogger(__name__)

try:
    from tqdm import tqdm as iter_with_progress
except ImportError:

    def iter_with_progress(iterable, **kwargs):
        return iterable


def convert_diskcache_storage(
    source_diskcache: diskcache.Cache,
    target_talsi: talsi.Storage,
    name_mapper: Callable[[str], tuple[str, str]],
    batch_size: int = 10_000,
    skip_existing: bool = False,
):
    batches_by_namespace = defaultdict(dict)
    n_total = len(source_diskcache)
    n_proc = 0
    for source_key in iter_with_progress(source_diskcache):
        namespace, key = name_mapper(source_key)
        if skip_existing and target_talsi.has(namespace, key):
            continue
        batch = batches_by_namespace[namespace]
        batch[key] = source_diskcache[source_key]
        n_proc += 1
        if len(batch) >= batch_size:
            target_talsi.set_many(namespace, batch)
            log.info(
                "%d/%d processed. Stored %d keys in namespace %r",
                n_proc,
                n_total,
                len(batch),
                namespace,
            )
            batch.clear()
    for namespace, batch in batches_by_namespace.items():
        if batch:
            target_talsi.set_many(namespace, batch)
            log.info("Stored %d final keys in namespace %r", len(batch), namespace)


def compare_diskcache_storage(
    source_diskcache: diskcache.Cache,
    talsi_store: talsi.Storage,
    name_mapper: Callable[[str], tuple[str, str]],
):
    for source_key in iter_with_progress(source_diskcache):
        namespace, key = name_mapper(source_key)
        source_value = source_diskcache[source_key]
        talsi_value = talsi_store.get(namespace, key)
        if talsi_value != source_value:
            yield (source_key, namespace, key)
            # fmt_talsi = pformat(talsi_value, sort_dicts=True)
            # fmt_source = pformat(source_value, sort_dicts=True)
            # for line in context_diff(fmt_talsi.splitlines(), fmt_source.splitlines()):
            #     print(line)
            # raise NotImplementedError(f"...")
