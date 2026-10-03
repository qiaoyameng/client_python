---
title: Summary
weight: 3
---

A Summary samples observations and tracks the total count and sum. Use it when
you want to track the size or duration of events and compute averages, but do not
need per-bucket breakdown or quantiles in your Prometheus queries.

Optionally, a Summary can also estimate quantiles (p50/p95/p99, ...) locally
over a sliding time window — see [Quantiles](#quantiles) below. If you need
quantiles that are aggregatable across instances, use a
[Histogram](../histogram/) instead.

```python
from prometheus_client import Summary
s = Summary('request_latency_seconds', 'Description of summary')
s.observe(4.7)    # Observe 4.7 (seconds in this case)
```

A Summary exposes two time series per metric:
- `<name>_count` — total number of observations
- `<name>_sum` — sum of all observed values

## Constructor

```python
Summary(name, documentation, labelnames=(), namespace='', subsystem='', unit='', registry=REGISTRY, quantiles=(), max_age=600.0, age_buckets=5, clock=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Metric name. |
| `documentation` | `str` | required | Help text shown in the `/metrics` output and Prometheus UI. |
| `labelnames` | `Iterable[str]` | `()` | Names of labels for this metric. See [Labels](../labels/). Note: `quantile` is reserved and cannot be used as a label name. |
| `namespace` | `str` | `''` | Optional prefix. |
| `subsystem` | `str` | `''` | Optional middle component. |
| `unit` | `str` | `''` | Optional unit suffix appended to the metric name. |
| `registry` | `CollectorRegistry` | `REGISTRY` | Registry to register with. Pass `None` to skip registration, which is useful in tests where you create metrics without wanting them in the global registry. |
| `quantiles` | `Sequence[Tuple[float, float]]` | `()` | Optional `(quantile, allowed_rank_error)` targets, e.g. `[(0.5, 0.05), (0.9, 0.01), (0.99, 0.001)]`. Empty disables quantile tracking. |
| `max_age` | `float` | `600.0` | Nominal size of the sliding quantile window, in seconds. |
| `age_buckets` | `int` | `5` | Number of time slices approximating the window. |
| `clock` | `Callable[[], float]` | `time.time` | Clock used for window rotation; injectable for tests. |

`namespace`, `subsystem`, and `name` are joined with underscores to form the full metric name:

```python
# namespace='myapp', subsystem='worker', name='task_duration_seconds'
# produces: myapp_worker_task_duration_seconds
Summary('task_duration_seconds', 'Task duration', namespace='myapp', subsystem='worker')
```

## Methods

### `observe(amount)`

Record a single observation. The amount is typically positive or zero.

```python
s.observe(0.43)   # observe 430ms
s.observe(1024)   # observe 1024 bytes
```

### `time()`

Observe the duration in seconds of a block of code or function and add it to the
summary. Every call accumulates — unlike `Gauge.time()`, which only keeps the
most recent duration. Can be used as a decorator or context manager.

```python
@s.time()
def process():
    pass

with s.time():
    pass

with s.time() as t:
    pass
print(t.duration) # observed time in seconds.
```

## Quantiles

Pass `quantiles` to estimate quantiles over a sliding time window:

```python
from prometheus_client import Summary

s = Summary('request_latency_seconds', 'Request latency',
            quantiles=[(0.5, 0.05), (0.9, 0.01), (0.99, 0.001)],
            max_age=600, age_buckets=5)
```

Each `(quantile, error)` pair guarantees that the reported value's true rank
within the window is inside `quantile ± error` (for example, the `0.9` value
above is between the 89th and 91st percentile of the window). Estimates use a
bounded CKMS summary — observations are compressed, not stored — so memory
stays small regardless of how many observations are made.

The sliding window is approximated by `age_buckets` rotating time slices, so
an observation influences quantiles for between `max_age - max_age/age_buckets`
and `max_age` seconds. The effective bounds are exposed as the `max_age`,
`age_buckets`, and `max_age_error` (`max_age / age_buckets`) attributes.

Quantile samples are exposed with a `quantile` label alongside the usual
`_count` and `_sum` series:

```
request_latency_seconds{quantile="0.5"} 0.21
request_latency_seconds{quantile="0.9"} 0.83
request_latency_seconds{quantile="0.99"} 1.4
request_latency_seconds_count 520
request_latency_seconds_sum 132.7
```

Notes:

- While the window holds no observations, quantile samples are reported as
  `NaN`. `_count` and `_sum` are cumulative and are never reset by window
  rotation.
- Observations must be finite when quantile tracking is enabled; `NaN` and
  infinities raise `ValueError`.
- Quantile tracking is only available in single-process mode; passing
  `quantiles` in multiprocess mode raises `ValueError` at construction.

## Labels

See [Labels](../labels/) for how to use `.labels()`, `.remove()`, `.remove_by_labels()`, and `.clear()`.

## Real-world example

Tracking the duration of background tasks:

```python
from prometheus_client import Summary, start_http_server

TASK_DURATION = Summary(
    'task_duration_seconds',
    'Time spent processing background tasks',
    labelnames=['task_type'],
    namespace='myapp',
)

def run_task(task_type, task):
    with TASK_DURATION.labels(task_type=task_type).time():
        # ... run the task ...
        pass

if __name__ == '__main__':
    start_http_server(8000)  # exposes metrics at http://localhost:8000/metrics
    # ... start your application ...
```

This produces:
```
myapp_task_duration_seconds_count{task_type="email"} 120
myapp_task_duration_seconds_sum{task_type="email"} 48.3
```

You can compute the average duration in PromQL as:
```
rate(myapp_task_duration_seconds_sum[5m]) / rate(myapp_task_duration_seconds_count[5m])
```
