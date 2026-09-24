---
title: Registry view
weight: 9
---

A `RegistryView` wraps any collector or registry and re-exposes its metrics
with a name prefix and a set of const (fixed) labels applied. This is useful
when several components each maintain their own `CollectorRegistry` and a
gateway wants to expose all of them through a single endpoint, with each
component's metrics namespaced and labelled.

The view is read-only: the wrapped source and the objects it returns are
never modified, so multiple views (including nested views) can wrap the same
registry independently.

```python
from prometheus_client import CollectorRegistry, Counter, generate_latest, RegistryView

component_registry = CollectorRegistry()
Counter('requests', 'Total requests', registry=component_registry).inc()

view = RegistryView(component_registry, prefix='auth_', const_labels={'component': 'auth'})
print(generate_latest(view).decode())
```

```python
# HELP auth_requests_total Total requests
# TYPE auth_requests_total counter
auth_requests_total{component="auth"} 1.0
```

The prefix is applied to the metric family name, keeping type suffixes in
place: a counter `requests` becomes `auth_requests_total` /
`auth_requests_created`, a histogram keeps its `_bucket` / `_sum` / `_count`
samples, and an info metric keeps its `_info` suffix. Exemplars, native
histograms and timestamps are carried over unchanged.

## Constructor

```python
RegistryView(source, prefix='', const_labels=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | collector | required | Anything with a no-argument `collect()` method: a `CollectorRegistry`, another `RegistryView`, or a custom collector. |
| `prefix` | `str` | `''` | Prefix prepended to every metric family name. |
| `const_labels` | `Dict[str, str]` | `None` | Labels added to every sample. |

## Collisions

Collisions are never resolved silently:

* If a const label name already exists on a sample, `collect()` raises
  `ConstLabelCollisionError`.
* If the transformation maps two metric families, or samples of two
  different families, onto the same name, `collect()` raises
  `DuplicateMetricNameError`.

Both are subclasses of `ValueError` and live in
`prometheus_client.registry_view`.

## Aggregation and exposition

A view can be used anywhere a registry is accepted, including
`generate_latest`, `make_wsgi_app` and `start_http_server`, and it can be
registered into another `CollectorRegistry` to build an aggregate endpoint:

```python
from prometheus_client import CollectorRegistry, RegistryView, start_http_server

gateway = CollectorRegistry()
gateway.register(RegistryView(auth_registry, prefix='auth_', const_labels={'component': 'auth'}))
gateway.register(RegistryView(billing_registry, prefix='billing_', const_labels={'component': 'billing'}))

start_http_server(8000, registry=gateway)
```

## Restricted collection

`view.restricted_registry(names)` takes names in the view's (prefixed)
namespace, maps them back to the source, and only collects from the
originating collectors. This also makes the `name[]` query parameter work
when the view is served by the built-in HTTP exposition.

```python
generate_latest(view.restricted_registry(['auth_requests_total']))
```
