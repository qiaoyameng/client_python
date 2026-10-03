---
title: Summary
weight: 3
---

A Summary samples observations and tracks the total count and sum. It can also
compute a bounded set of recent quantiles in single-process mode. Quantiles are
approximated with a bounded, mergeable in-memory summary; the client never keeps
all observations forever.

Use it for request latencies or request sizes where you need local p50/p95/p99
values. Use a [Histogram](../histogram/) when quantiles must be aggregated across
processes or scraped instances.

```python
from prometheus_client import Summary
s = Summary('request_latency_seconds', 'Description of summary')
s.observe(4.7)
```

A Summary always exposes:
- `<name>_count` — total number of observations
- `<name>_sum` — sum of all observations

When recent quantiles are enabled it additionally exposes:
- `<name>{quantile="..."}` — approximate quantile from the configured window

## Constructor

```python
Summary(
    name,
    documentation,
    labelnames=(),
    namespace='',
    subsystem='',
    unit='',
    registry=REGISTRY,
    quantiles=None,
    rank_error=0.01,
    max_age_seconds=600.0,
    time_slices=5,
    clock=time.monotonic,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Metric name. |
| `documentation` | `str` | required | Help text. |
| `labelnames` | `Iterable[str]` | `()` | Labels for this metric. `quantile` is reserved. |
| `namespace` | `str` | `''` | Optional prefix. |
| `subsystem` | `str` | `''` | Optional middle component. |
| `unit` | `str` | `''` | Optional unit suffix. |
| `registry` | `CollectorRegistry` | `REGISTRY` | Registry. Pass `None` to skip registration. |
| `quantiles` | sequence of floats or mapping | `None` | Quantiles to expose, such as `(0.5, 0.9, 0.99)`. A mapping can give each quantile its own rank error, e.g. `{0.5: 0.01, 0.99: 0.001}`. |
| `rank_error` | `float` | `0.01` | Maximum rank error used for all quantiles unless overridden by the mapping form. |
| `max_age_seconds` | `float` | `600.0` | Nominal recent-window duration. |
| `time_slices` | `int` | `5` | Number of fixed time slices used to approximate the window. |
| `clock` | callable | `time.monotonic` | Injectable zero-argument clock returning finite seconds. Tests should use a fake clock instead of sleeping. |

### Window approximation and expiration

The window is implemented with `time_slices` fixed-width tumbling slices. Its
public effective range is:

```text
(max_age_seconds - max_age_seconds / time_slices, max_age_seconds]
```

`max_expiration_error_seconds = max_age_seconds / time_slices` is exposed on the
metric and describes the maximum possible discrepancy between an observation's
nominal and effective age. Increasing `time_slices` reduces that error at the cost
of more in-memory summaries.

The slices are rotated lazily. A call whose injected clock jumps forward creates
or expires as many slices as needed without iterating over the skipped interval.
Clock values must be finite and non-decreasing.

## Methods

### `observe(amount)`

Record one observation. Negative and duplicate values are accepted, matching the
existing cumulative `_sum` semantics. Non-finite values (`NaN`, `+Inf`, and
`-Inf`) raise `ValueError`.

Quantiles are based only on observations still inside the recent window. The
cumulative `_count` and `_sum` cover the lifetime of the process and are not
reset when the window becomes empty.

### `time()`

Observe the duration in seconds of a block or decorated function.

```python
@s.time()
def process():
    pass

with s.time():
    pass
```

## Empty windows

If no observations are inside the configured effective window, each quantile
sample is emitted as `NaN`. `_count` and `_sum` keep their lifetime totals.

## Multi-process mode

Recent quantiles are not supported when `PROMETHEUS_MULTIPROC_DIR` is set. A
`Summary` with non-empty `quantiles` raises `ValueError` during construction in
multi-process mode. Quantile-less summaries continue to aggregate count and sum as
before.

## Labels

See [Labels](../labels/) for `.labels()`, `.remove()`, `.remove_by_labels()`, and
`.clear()`.

## Example

```python
REQUEST_LATENCY = Summary(
    'request_latency_seconds',
    'Request latency',
    quantiles=(0.5, 0.9, 0.99),
    rank_error=0.005,
    max_age_seconds=300,
    time_slices=10,
)
```

This exposes recent 50th, 90th, and 99th percentile values. All quantile
samples from one scrape are computed from the same rotated-slice snapshot.
