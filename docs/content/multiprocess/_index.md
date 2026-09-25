---
title: Multiprocess Mode
weight: 5
---

Prometheus client libraries presume a threaded model, where metrics are shared
across workers. This doesn't work so well for languages such as Python where
it's common to have processes rather than threads to handle large workloads.

To handle this the client library can be put in multiprocess mode.
This comes with a number of limitations:

- Registries can not be used as normal:
  - all instantiated metrics are collected
  - Registering metrics to a registry later used by a `MultiProcessCollector`
    may cause duplicate metrics to be exported
  - Filtering on metrics works if and only if the constructor was called with
    `support_collectors_without_names=True` and it but might be inefficient.
- Custom collectors do not work (e.g. cpu and memory metrics)
- Gauges cannot use `set_function`
- Info and Enum metrics do not work
- The pushgateway cannot be used
- Gauges cannot use the `pid` label
- Exemplars are not supported

There's several steps to getting this working:

**1. Deployment**:

The `PROMETHEUS_MULTIPROC_DIR` environment variable must be set to a directory
that the client library can use for metrics. This directory must be wiped
between process/Gunicorn runs (before startup is recommended).

This environment variable should be set from a start-up shell script,
and not directly from Python (otherwise it may not propagate to child processes).

Note: on Windows Subsystem for Linux (WSL), set `PROMETHEUS_MULTIPROC_DIR` to a
Linux-native filesystem path (e.g. `/tmp` or `/home/<user>`) rather than a
Windows-mounted path (e.g. `/mnt/c/...`). On Windows-mounted filesystems the
per-process metric files can be written with an incorrect internal offset,
causing the collector to silently read no data.

**2. Metrics collector**:

The application must initialize a new `CollectorRegistry`, and store the
multi-process collector inside. It is a best practice to create this registry
inside the context of a request to avoid metrics registering themselves to a
collector used by a `MultiProcessCollector`. If a registry with metrics
registered is used by a `MultiProcessCollector` duplicate metrics may be
exported, one for multiprocess, and one for the process serving the request.

```python
from prometheus_client import multiprocess
from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST, Counter

MY_COUNTER = Counter('my_counter', 'Description of my counter')

# Expose metrics.
def app(environ, start_response):
    registry = CollectorRegistry(support_collectors_without_names=True)
    multiprocess.MultiProcessCollector(registry)
    data = generate_latest(registry)
    status = '200 OK'
    response_headers = [
        ('Content-type', CONTENT_TYPE_LATEST),
        ('Content-Length', str(len(data)))
    ]
    start_response(status, response_headers)
    return iter([data])
```

**3. Gunicorn configuration**:

The `gunicorn` configuration file needs to include the following function:

```python
from prometheus_client import multiprocess

def child_exit(server, worker):
    multiprocess.mark_process_dead(worker.pid)
```

**4. Metrics tuning (Gauge)**:

When `Gauge`s are used in multiprocess applications,
you must decide how to handle the metrics reported by each process.
Gauges have several modes they can run in, which can be selected with the `multiprocess_mode` parameter.

- 'all': Default. Return a timeseries per process (alive or dead), labelled by the process's `pid` (the label is added internally).
- 'min': Return a single timeseries that is the minimum of the values of all processes (alive or dead).
- 'max': Return a single timeseries that is the maximum of the values of all processes (alive or dead).
- 'sum': Return a single timeseries that is the sum of the values of all processes (alive or dead).
- 'mostrecent': Return a single timeseries that is the most recent value among all processes (alive or dead).

Prepend 'live' to the beginning of the mode to return the same result but only considering living processes
(e.g., 'liveall, 'livesum', 'livemax', 'livemin', 'livemostrecent').

```python
from prometheus_client import Gauge

# Example gauge
IN_PROGRESS = Gauge("inprogress_requests", "help", multiprocess_mode='livesum')
```

## API Reference

### `MultiProcessCollector(registry, path=None)`

Collector that aggregates metrics written by all processes in the multiprocess directory.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `registry` | `CollectorRegistry` | required | Registry to register with. Pass a registry created inside the request context to avoid duplicate metrics. |
| `path` | `Optional[str]` | `None` | Path to the directory containing the per-process metric files. Defaults to the `PROMETHEUS_MULTIPROC_DIR` environment variable. |

Raises `ValueError` if `path` is not set or does not point to an existing directory.

```python
from prometheus_client import multiprocess, CollectorRegistry

def app(environ, start_response):
    registry = CollectorRegistry(support_collectors_without_names=True)
    multiprocess.MultiProcessCollector(registry)
    ...
```

To use a custom path instead of the environment variable:

```python
collector = multiprocess.MultiProcessCollector(registry, path='/var/run/prom')
```

### `mark_process_dead(pid, path=None)`

Removes the per-process metric files for a dead process. Call this from your process manager
when a worker exits to prevent stale `live*` gauge values from accumulating.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pid` | `int` | required | PID of the process that has exited. |
| `path` | `Optional[str]` | `None` | Path to the multiprocess directory. Defaults to the `PROMETHEUS_MULTIPROC_DIR` environment variable. |

Returns `None`. Only removes files for `live*` gauge modes (e.g. `livesum`, `liveall`); files
for non-live modes are left in place so their last values remain visible until the directory is
wiped on restart.

```python
# Gunicorn config
from prometheus_client import multiprocess

def child_exit(server, worker):
    multiprocess.mark_process_dead(worker.pid)
```

## Removing and clearing label series

`remove()`, `remove_by_labels()` and `clear()` work in multiprocess mode. In
addition to dropping the child from the calling process, they delete the
series from **every** process data file in `PROMETHEUS_MULTIPROC_DIR`, so the
series disappears from collection immediately. All samples belonging to the
series are treated as one unit: every aggregation mode of a Gauge, the total
of a Counter, and the buckets/sum/count of a Histogram are removed together.

Deletion is tracked with a **deletion generation** counter stored in each data
file. A process that still holds a removed child object (a stale handle)
detects the generation change on its next write, discards its locally
accumulated value, and re-reads from the file. As a result a stale handle can
never resurrect the old value: a stale Counter or Gauge `inc()` only
contributes the delta accumulated since the deletion, and a Gauge `set()`
recreates the series with the new absolute value. Samples of a stale Histogram
handle are recreated lazily as they are touched; to fully re-create a series,
call `remove()` and then `.labels()` again, which re-initializes all of its
samples from zero.

Compatibility with files written by older versions of the library: they carry
deletion generation 0 and are read and deleted transparently. The reverse is
not true — deleted entries are tombstoned in place, a marker that versions
without removal support cannot parse. Upgrade all processes before using
`remove()`/`clear()` in multiprocess mode.
