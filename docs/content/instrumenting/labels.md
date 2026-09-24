---
title: Labels
weight: 7
---

All metrics can have labels, allowing grouping of related time series.

See the best practices on [naming](https://prometheus.io/docs/practices/naming/)
and [labels](https://prometheus.io/docs/practices/instrumentation/#use-labels).

Taking a counter as an example:

```python
from prometheus_client import Counter
c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'])
c.labels('get', '/').inc()
c.labels('post', '/submit').inc()
```

Labels can also be passed as keyword-arguments:

```python
from prometheus_client import Counter
c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'])
c.labels(method='get', endpoint='/').inc()
c.labels(method='post', endpoint='/submit').inc()
```

Metrics with labels are not initialized when declared, because the client can't
know what values the label can have. It is recommended to initialize the label
values by calling the `.labels()` method alone:

```python
from prometheus_client import Counter
c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'])
c.labels('get', '/')
c.labels('post', '/submit')
```

## Removing labelsets

### `remove(*labelvalues)`

Remove a specific labelset from the metric. Values must be passed in the same
order as `labelnames` were declared.

```python
c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'])
c.labels('get', '/').inc()
c.remove('get', '/')
```

### `remove_by_labels(labels)`

Remove all labelsets that partially match the given dict of label names and values.

```python
c.remove_by_labels({'method': 'get'})  # removes all labelsets where method='get'
```

### `clear()`

Remove all labelsets from the metric at once.

```python
c.clear()
```

## Limiting label cardinality

If label values can be influenced by user input, the number of label
combinations (and thus time series) can grow without bound. `Counter` and
`Histogram` support an optional hard limit via `max_labelsets`, together with
a caller-configured `overflow_labelvalues` combination:

```python
from prometheus_client import Counter
c = Counter('my_requests_total', 'HTTP Failures', ['method', 'endpoint'],
            max_labelsets=100, overflow_labelvalues=('other', 'other'))
```

The first `max_labelsets` distinct combinations are tracked precisely and are
never evicted. Once the limit is reached, `.labels()` calls with new
combinations return the child for `overflow_labelvalues`, so those
observations are aggregated into that single overflow series. The overflow
combination does not count against the limit, and is exported with the
configured label values, so choose values that cannot be confused with real
label values.

`remove()` and `clear()` recycle capacity: removing a combination frees its
slot for a new combination, and if the overflow combination itself is removed
it is recreated on demand. `clear()` removes everything, including the
overflow child.

This is not supported for `Gauge` or in multiprocess mode; configuring it
there raises a `ValueError`.
