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

## Limiting the number of labelsets

`Counter` and `Histogram` accept an optional `max_labelsets` limit, which must
be configured together with `overflow_labels`. Once `max_labelsets` distinct
labelsets have been admitted, observations for any new labelset are aggregated
into the single overflow labelset instead of creating a new child. This
protects against unbounded cardinality when label values come from an
untrusted source, such as user input.

```python
from prometheus_client import Counter
c = Counter('my_requests_total', 'HTTP Failures', ['user'],
            max_labelsets=1000, overflow_labels={'user': '__overflow__'})
```

`overflow_labels` must be a dict keyed by exactly the metric's label names.
Choose values that cannot collide with real label values, as the overflow
labelset is exported like any other series. Requesting the overflow labelset
directly via `.labels()` returns the same overflow child and does not consume
an admission slot.

Admitted labelsets keep their own child and are never evicted, so their
values stay exact. The limit is enforced atomically under a lock, so
concurrent first-time `.labels()` calls can never exceed it, and a given
labelset always returns the same child object.

Removing a labelset with `remove()` or `remove_by_labels()` frees its slot,
allowing a new labelset to be admitted. `clear()` frees every slot, including
the overflow labelset; the overflow child is recreated on the next
overflowing observation.

`max_labelsets` is not supported for `Gauge` or in multiprocess mode;
configuring it there raises a `ValueError`.
