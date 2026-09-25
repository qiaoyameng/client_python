---
title: Histogram
weight: 4
---

A Histogram samples observations and counts them in configurable buckets. Use it
when you want to track distributions — request latency, response sizes — and need
to calculate quantiles (p50, p95, p99) in your queries.

```python
from prometheus_client import Histogram
h = Histogram('request_latency_seconds', 'Description of histogram')
h.observe(4.7)    # Observe 4.7 (seconds in this case)
```

A Histogram exposes three time series per metric:
- `<name>_bucket{le="<bound>"}` — count of observations with value ≤ le (cumulative)
- `<name>_sum` — sum of all observed values
- `<name>_count` — total number of observations

## Constructor

```python
Histogram(name, documentation, labelnames=(), namespace='', subsystem='', unit='', registry=REGISTRY, buckets=DEFAULT_BUCKETS)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Metric name. |
| `documentation` | `str` | required | Help text shown in the `/metrics` output and Prometheus UI. |
| `labelnames` | `Iterable[str]` | `()` | Names of labels for this metric. See [Labels](../labels/). Note: `le` is reserved and cannot be used as a label name. |
| `namespace` | `str` | `''` | Optional prefix. |
| `subsystem` | `str` | `''` | Optional middle component. |
| `unit` | `str` | `''` | Optional unit suffix appended to the metric name. |
| `registry` | `CollectorRegistry` | `REGISTRY` | Registry to register with. Pass `None` to skip registration, which is useful in tests where you create metrics without wanting them in the global registry. |
| `buckets` | `Sequence[float]` | `DEFAULT_BUCKETS` | Upper bounds of the histogram buckets. Must be in ascending order. `+Inf` is always appended automatically. |

`namespace`, `subsystem`, and `name` are joined with underscores to form the full metric name:

```python
# namespace='myapp', subsystem='http', name='request_duration_seconds'
# produces: myapp_http_request_duration_seconds
Histogram('request_duration_seconds', 'Latency', namespace='myapp', subsystem='http')
```

Default buckets are intended to cover typical web/RPC request latency in seconds and are
accessible as `Histogram.DEFAULT_BUCKETS`:

```
.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, +Inf
```

To override with buckets tuned to your workload:

```python
h = Histogram('request_latency_seconds', 'Latency', buckets=[.1, .5, 1, 2, 5])
```

## Methods

### `observe(amount, exemplar=None)`

Record a single observation. The amount is typically positive or zero.

```python
h.observe(0.43)   # observe 430ms
```

To attach trace context to an observation, pass an `exemplar` dict. Exemplars are
only rendered in OpenMetrics format. See [Exemplars](../exemplars/) for details.

```python
h.observe(0.43, exemplar={'trace_id': 'abc123'})
```

### `time()`

Observe the duration in seconds of a block of code or function and add it to the
histogram. Every call accumulates — unlike `Gauge.time()`, which only keeps the
most recent duration. Can be used as a decorator or context manager.

```python
@h.time()
def process():
    pass

with h.time():
    pass

with h.time() as t:
    pass
print(t.duration) # observed time in seconds.
```

## Labels

See [Labels](../labels/) for how to use `.labels()`, `.remove()`, `.remove_by_labels()`, and `.clear()`.

## Real-world example

Tracking HTTP request latency with custom buckets tuned to the workload:

```python
from prometheus_client import Histogram, start_http_server

REQUEST_LATENCY = Histogram(
    'request_duration_seconds',
    'HTTP request latency',
    labelnames=['method', 'endpoint'],
    namespace='myapp',
    buckets=[.01, .05, .1, .25, .5, 1, 2.5, 5],
)

def handle_request(method, endpoint):
    with REQUEST_LATENCY.labels(method=method, endpoint=endpoint).time():
        # ... handle the request ...
        pass

if __name__ == '__main__':
    start_http_server(8000)  # exposes metrics at http://localhost:8000/metrics
    # ... start your application ...
```

This produces time series like:
```
myapp_request_duration_seconds_bucket{method="GET",endpoint="/api/users",le="0.1"} 42
myapp_request_duration_seconds_sum{method="GET",endpoint="/api/users"} 3.7
myapp_request_duration_seconds_count{method="GET",endpoint="/api/users"} 50
```

## Native histograms

`NativeHistogramMetric` is an experimental metric type that tracks observations
in exponential buckets instead of explicitly configured ones. It maintains the
bucket counts, a zero bucket, and the total count and sum of observations, and
exposes them as a single native histogram sample.

```python
from prometheus_client import NativeHistogramMetric
h = NativeHistogramMetric('request_latency_seconds', 'Description of histogram', schema=3)
h.observe(0.25)
```

### Bucket layout

With the configured `schema`, the bucket growth factor (base) is
`2**(2**-schema)`, and bucket `i` covers `[base**(i-1), base**i)` — the lower
boundary is inclusive, the upper boundary exclusive. Higher schemas give finer
buckets. Valid schemas are integers from -4 to 8 (default 3, a growth factor of
about 1.09). Positive and negative observations are tracked in separate,
mirrored bucket structures.

### Zero bucket

Observations whose absolute value is less than or equal to `zero_threshold`
(default `2**-128`) are counted in the zero bucket instead of an exponential
bucket. The zero threshold is fixed at construction time and cannot be changed
afterwards — observations already counted in the zero bucket could not be
redistributed — and it is not affected by resolution reductions. If a different
zero threshold is needed, create a new metric.

### Bucket count limit

The number of populated buckets per side (positive/negative) is limited to
`max_buckets` (default 100, `None` disables the limit). When an observation
would exceed the limit, the schema is reduced by 1 — doubling the bucket width
and merging adjacent bucket pairs — until both sides fit again or the minimum
schema of -4 is reached. Merging never loses observation counts.

### NaN and infinite observations

NaN and infinite observations are only added to the total count and sum of
observations (the sum becomes NaN or infinite accordingly); they are never
added to any bucket.

### Caveats

Native histograms are only exposed via the OpenMetrics format (version 2.0.0
and above); the classic text format cannot represent them and skips them.
Native histograms are single-process only and do not work in multiprocess mode.
