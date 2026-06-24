# Benchmarks

 Each benchmark reports the
**median of repeated trials** (with standard deviation where relevant); the
two microbenchmarks (`put`, `put_get_ack`) use [`pyperf`](https://pyperf.readthedocs.io/),
which calibrates loop counts and runs multiple worker processes.

Unless a row says otherwise, EQueue runs with `sync=False` (writes reach the OS
page cache, no `fsync`) and background threads disabled, so the timings isolate
the queue operation itself.

| Item        | Value                              |
|-------------|------------------------------------|
| CPU         | AMD Ryzen 7 7700 (8-core, 3800 MHz) |
| GPU         | NVIDIA GeForce RTX 5070 OC 12 GB   |
| RAM         | 32 GB                              |
| OS          | Windows 11 Home (Build 26200)      |
| Python      | 3.13 (CPython)                     |
| LMDB binding | lmdb 2.2.0                         |
| persist-queue | 1.1.0                            |
| Storage     | NVMe SSD, NTFS                     |

| Scenario | Result |
| --- | --- |
| `put()` (no fsync) | 5.3 us (~190,000/sec) |
| `put+get+ack` round-trip (no fsync) | 17.6 us (~57,000/sec) |
| Concurrent claim+ack, 1–8 threads | ~79,000–81,000 jobs/sec |
| Recovery per job | ~2.0 us |
| Vacuum per job | ~1.35 us |
| Python heap per in-flight Record | ~494 bytes |
| vs persist-queue, **durable** (both fsync) | 4.8x faster `put()`, 5.1x round-trip |
| vs persist-queue, **fast** (neither fsync) | 4.1x faster `put()`, 4.4x round-trip |

---

## put()

`bench_put.py` calls `put()` in a tight loop. Each call opens one LMDB write
transaction and writes three keys (job, state, queued index).

```
put: Mean +- std dev: 5.27 us +- 0.15 us
```

---

## put+get+ack round-trip

`bench_put_get_ack.py` does one `put`, one `get`, and one `ack` per iteration.
The queue stays at most one item deep throughout.

```
put_get_ack: Mean +- std dev: 17.6 us +- 0.5 us
```

The ~12 us on top of `put()` covers the claim transaction and the ack transaction.

---

## Concurrent workers

`bench_concurrent.py` pre-fills 2,000 jobs, then starts N threads that each claim
and ack until the queue is empty. Throughput is measured as
`jobs / (time of last claim start)`; the timestamp of the final claim is
captured under the shared counter lock, so the result **excludes** the trailing
`QueueEmpty` poll wait each worker pays while draining.

```
threads    jobs/sec
1          81,172
2          81,501
4          80,881
8          78,925
```

LMDB allows a single writer at a time, so the claim transactions serialize and
throughput stays essentially flat (a slight dip at 8 threads from lock
contention). Adding threads does not multiply claim throughput; the win from
concurrency comes from overlapping the *work* workers do between claims, not the
claims themselves.

---

## Recovery

`bench_recovery.py` measures `_recover()` for N expired in-flight jobs (the
one-off startup cost after a crash).

```
jobs  median (ms)  per job (us)
100   0.24         2.39
500   1.01         2.03
1000  2.03         2.03
5000  10.41        2.08
```

About 2 us per job, linear in the number of jobs. A crash with 1,000 in-flight
jobs adds ~2 ms to startup.

---

## Vacuum

`bench_vacuum.py` measures `_vacuum()` for N completed (DONE) jobs.

```
jobs  median (ms)  per job (us)
100   0.13         1.33
500   0.68         1.35
1000  1.34         1.34
5000  6.76         1.35
```

About 1.35 us per job, linear in the number of DONE jobs.

---

## Python heap per Record

`mem_trace.py` uses `tracemalloc`. LMDB memory-mapped pages live outside the
Python heap and are not counted here.

```
stage                    heap      delta vs baseline
baseline                 0.0 KB    0.0 KB
after 1000 puts          9.3 KB    9.3 KB
after 1000 gets (held)   491.9 KB  491.9 KB
after 1000 acks (freed)  70.4 KB   70.4 KB
approx per held Record:  494 bytes
```

Each in-flight `Record` costs about 494 bytes: ~72 B for the dataclass, ~255 B
for the `partial` completion closure, ~58 B for the decoded payload string (17
chars in this test), ~49 B for the claim token.

---

## vs persist-queue

`bench_vs_persistqueue.py` compares EQueue against
[persist-queue](https://pypi.org/project/persist-queue/) `SQLiteAckQueue`
(SQLite-backed). **The comparison is run at matched durability**, the
two libraries default to different settings:

- persist-queue runs SQLite in WAL mode and leaves `synchronous=FULL`, so it
  `fsync`s on every commit.
- EQueue only `fsync`s when constructed with `sync=True`.

The table below instead pairs like with like:

- **durable**: both `fsync` on every commit (EQueue `sync=True`; persist-queue
  default WAL + `synchronous=FULL`).
- **fast**: neither `fsync`s (EQueue `sync=False`; persist-queue
  `synchronous=OFF`, set via `PRAGMA`).

Each cell is the median of 7 trials (fresh queue per trial), 2,000 ops each.

```
regime           scenario     equeue (us)     persist-queue (us)  speedup
durable (fsync)  put          127.2 +/- 10.7  608.8 +/- 9.7       4.8x
durable (fsync)  put+get+ack  353.4 +/- 44.5  1798.1 +/- 24.1     5.1x
fast (no fsync)  put          5.0 +/- 0.0     20.6 +/- 1.3        4.1x
fast (no fsync)  put+get+ack  15.9 +/- 0.2    70.3 +/- 7.6        4.4x
```

**Result:** at equal durability, EQueue is **4–5x faster** than
persist-queue across both scenarios. LMDB uses memory-mapped, copy-on-write
B-tree writes; SQLite writes a WAL frame and updates a B-tree per operation. The
gap is consistent whether or not `fsync` is enabled.

---

## How to run

Benchmark dependencies (`pyperf`, `persist-queue`) are in a separate group and
are not installed by `uv sync`. Install them once with:

```bash
uv sync --group benchmarks
```

Then run:

```bash
uv run --group benchmarks python benchmarks/bench_put.py
uv run --group benchmarks python benchmarks/bench_put_get_ack.py
uv run --group benchmarks python benchmarks/bench_vs_persistqueue.py
uv run python benchmarks/bench_concurrent.py
uv run python benchmarks/bench_recovery.py
uv run python benchmarks/bench_vacuum.py
uv run python benchmarks/mem_trace.py
```

Pass `--rigorous` to the two `pyperf` scripts for longer, lower-noise runs.
`bench_concurrent.py`, `bench_recovery.py`, `bench_vacuum.py`, and `mem_trace.py`
only use the standard library and `equeue`, so they work without the extra group.
