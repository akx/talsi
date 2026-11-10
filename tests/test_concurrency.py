import threading

import talsi


def test_concurrent_reads_and_writes(tmp_path):
    """Calls from multiple threads must not trip over each other (e.g. "Already borrowed")."""
    storage = talsi.Storage(str(tmp_path / "concurrent.db"))
    big_value = b"x" * (1 << 20)  # large enough that we spend time with the GIL released
    storage.set("ns", "big", big_value)
    errors = []
    barrier = threading.Barrier(8)

    def worker(n: int):
        barrier.wait()
        try:
            for i in range(50):
                assert storage.get("ns", "big") == big_value
                assert storage.get_many("ns", ["big"]) == {"big": big_value}
                assert storage.has("ns", "big")
                storage.set("ns", f"k{n}-{i}", i)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    assert len(storage.list_keys("ns", like="k%")) == 8 * 50
