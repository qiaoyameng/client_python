"""Strict, validated snapshot export.

The regular export path (:func:`prometheus_client.exposition.generate_latest`)
streams metrics straight from the collectors to the encoder. A single broken
custom collector can therefore invalidate a whole scrape, and Prometheus only
reports the scrape as failed without pointing at the offending data.

This module offers an explicitly strict alternative: metrics are collected
from every collector exactly once, validated as a whole snapshot, and only
then handed to the existing encoders. The caller chooses whether a validation
problem fails the whole scrape (:data:`FAIL`) or isolates the offending
metric families while the rest is still exported (:data:`SKIP`).

Input metrics and samples are never modified, and legal special floating
point values (NaN, +Inf, -Inf) are preserved.
"""

from typing import Dict, Iterable, List, Optional, Tuple

from .metrics_core import METRIC_TYPES, Metric
from .openmetrics import exposition as openmetrics
from .registry import Collector, CollectorRegistry, REGISTRY

FAIL = 'fail'
"""Validation strategy: any problem fails the whole scrape."""

SKIP = 'skip'
"""Validation strategy: invalid metric families are dropped, the rest is exported."""

_STRATEGIES = (FAIL, SKIP)


class SnapshotValidationError(ValueError):
    """A validation problem found in collected metrics.

    Carries the identity of the collector that produced the offending
    data, the metric family name and the sample labels, when known.
    """

    def __init__(self,
                 message: str,
                 collector: Optional[str] = None,
                 metric: Optional[str] = None,
                 labels: Optional[Dict[str, str]] = None,
                 ) -> None:
        self.collector: Optional[str] = collector
        self.metric: Optional[str] = metric
        self.labels: Optional[Dict[str, str]] = labels
        context = []
        if collector is not None:
            context.append(f'collector={collector}')
        if metric is not None:
            context.append(f'family={metric}')
        if labels is not None:
            context.append(f'labels={labels}')
        if context:
            message = '{} ({})'.format(message, ', '.join(context))
        super().__init__(message)


class Snapshot:
    """An immutable, validated set of metric families.

    Implements the Collector protocol, so it can be passed directly to
    :func:`prometheus_client.exposition.generate_latest` and the other
    existing encoders. ``errors`` holds the validation problems that were
    isolated instead of raised (only non-empty with the ``SKIP`` strategy).
    """

    def __init__(self, metrics: Iterable[Metric], errors: Iterable[SnapshotValidationError] = ()) -> None:
        self.metrics: Tuple[Metric, ...] = tuple(metrics)
        self.errors: Tuple[SnapshotValidationError, ...] = tuple(errors)

    def collect(self) -> Iterable[Metric]:
        return iter(self.metrics)


def _collector_id(collector: Collector) -> str:
    return f'{type(collector).__module__}.{type(collector).__qualname__}@{id(collector):#x}'


def _exported_family_name(metric: Metric) -> str:
    # Mirrors the name munging done by the text format encoders.
    if metric.type == 'counter':
        return metric.name + '_total'
    if metric.type == 'info':
        return metric.name + '_info'
    return metric.name


def _label_key(labels: Dict[str, str], without: Optional[str] = None) -> frozenset:
    return frozenset((k, v) for k, v in labels.items() if k != without)


def collect_snapshot(source: Collector = REGISTRY,
                     on_error: str = FAIL,
                     escaping: str = openmetrics.UNDERSCORES) -> Snapshot:
    """Collect metrics from every collector exactly once and validate them.

    Params:
        source: A CollectorRegistry or any object with a ``collect`` method.
        on_error: ``FAIL`` to raise on the first problem, ``SKIP`` to
            isolate the offending metric families and keep the rest.
        escaping: Escaping scheme the snapshot will be encoded with, used
            to detect names that collide after format escaping.

    Returns: a Snapshot usable anywhere a Collector is accepted.

    Raises: SnapshotValidationError if on_error is FAIL and a problem is found.
    """
    if on_error not in _STRATEGIES:
        raise ValueError(f'on_error must be one of {_STRATEGIES}')

    errors: List[SnapshotValidationError] = []
    bad_family_ids = set()

    def report(message: str, collector: Optional[str] = None, metric: Optional[str] = None,
               labels: Optional[Dict[str, str]] = None, families: Iterable[Metric] = ()) -> None:
        error = SnapshotValidationError(message, collector=collector, metric=metric, labels=labels)
        if on_error == FAIL:
            raise error
        errors.append(error)
        for family in families:
            bad_family_ids.add(id(family))

    if isinstance(source, CollectorRegistry):
        with source._lock:
            collectors = list(source._collector_to_names)
            target_info = source._target_info_metric() if source._target_info else None
    else:
        collectors = [source]
        target_info = None

    collected: List[Tuple[str, List[Metric]]] = []
    if target_info is not None:
        collected.append(('target_info', [target_info]))
    for collector in collectors:
        cid = _collector_id(collector)
        try:
            metrics = list(collector.collect())
        except Exception as exc:
            report(f'collector raised {type(exc).__name__}: {exc}', collector=cid)
            continue
        collected.append((cid, metrics))

    families = [(cid, metric) for cid, metrics in collected for metric in metrics]
    _validate_families(families, escaping, report)

    return Snapshot(
        (metric for _, metrics in collected for metric in metrics if id(metric) not in bad_family_ids),
        errors,
    )


def generate_latest_strict(source: Collector = REGISTRY,
                           escaping: str = openmetrics.UNDERSCORES,
                           on_error: str = FAIL) -> bytes:
    """Collect, validate and encode metrics in one strict step.

    Same output as :func:`prometheus_client.exposition.generate_latest`,
    but the metrics are validated as a snapshot before encoding.
    """
    from .exposition import generate_latest
    return generate_latest(collect_snapshot(source, on_error=on_error, escaping=escaping), escaping)


def _validate_families(families: List[Tuple[str, Metric]], escaping: str, report) -> None:
    family_by_name: Dict[str, Tuple[str, Metric]] = {}
    exported_names: Dict[str, Tuple[str, str, Metric]] = {}
    seen_samples: Dict[tuple, Tuple[str, Metric]] = {}
    escaped_samples: Dict[str, Tuple[str, str, Metric]] = {}

    for cid, metric in families:
        name = getattr(metric, 'name', None)
        mtype = getattr(metric, 'type', None)
        if not isinstance(name, str) or not name:
            report('metric family has no valid name', collector=cid, families=[metric])
            continue
        if mtype not in METRIC_TYPES:
            report(f'invalid metric type {mtype!r}', collector=cid, metric=name, families=[metric])
            continue
        if not isinstance(getattr(metric, 'documentation', None), str):
            report('metric family documentation is not a string', collector=cid, metric=name, families=[metric])
            continue

        previous = family_by_name.get(name)
        if previous is not None and previous[1] is not metric:
            other_cid, other = previous
            conflict = f'duplicate metric family name {name!r}'
            if other.type != mtype:
                conflict += f' with conflicting types {other.type!r} vs {mtype!r}'
            report(conflict, collector=cid, metric=name, families=[metric, other])
        else:
            family_by_name[name] = (cid, metric)

        exported = _exported_family_name(metric)
        escaped = openmetrics.escape_metric_name(exported, escaping)
        previous = exported_names.get(escaped)
        if previous is not None and previous[0] != exported:
            report(f'metric family names {previous[0]!r} and {exported!r} collide after escaping as {escaped!r}',
                   collector=cid, metric=name, families=[metric, previous[2]])
        else:
            exported_names.setdefault(escaped, (exported, cid, metric))

        samples = getattr(metric, 'samples', None)
        if samples is None:
            report('metric family has no samples', collector=cid, metric=name, families=[metric])
            continue
        for sample in samples:
            _validate_sample(cid, metric, sample, escaping, report, seen_samples, escaped_samples)

        if mtype in ('histogram', 'gaugehistogram'):
            _validate_histogram(cid, metric, report)
        elif mtype == 'summary':
            _validate_summary(cid, metric, report)

    # Sample names must not collide with a different family's name.
    for cid, metric in families:
        name = getattr(metric, 'name', None)
        samples = getattr(metric, 'samples', None)
        if not isinstance(name, str) or samples is None:
            continue
        for sample in samples:
            sample_name = getattr(sample, 'name', None)
            if not isinstance(sample_name, str):
                continue
            for names in (family_by_name, exported_names):
                other = names.get(sample_name)
                if other is not None and other[-1] is not metric:
                    report(f'sample name {sample_name!r} collides with metric family name {other[-1].name!r}',
                           collector=cid, metric=name, labels=getattr(sample, 'labels', None),
                           families=[metric, other[-1]])


def _validate_sample(cid: str, metric: Metric, sample, escaping: str, report,
                     seen_samples: Dict[tuple, Tuple[str, Metric]],
                     escaped_samples: Dict[str, Tuple[str, str, Metric]]) -> None:
    try:
        name = sample.name
        labels = sample.labels
        value = sample.value
    except AttributeError:
        report(f'malformed sample {sample!r}', collector=cid, metric=metric.name, families=[metric])
        return

    if not isinstance(name, str) or not name:
        report(f'sample has no valid name: {name!r}', collector=cid, metric=metric.name, families=[metric])
        return
    if not isinstance(labels, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in labels.items()):
        report(f'sample {name!r} has invalid labels {labels!r}', collector=cid, metric=metric.name, families=[metric])
        return
    try:
        # NaN and +/-Inf are legal sample values and are preserved; only
        # values that are not numbers at all are rejected here.
        float(value)
    except (TypeError, ValueError):
        report(f'sample {name!r} value {value!r} is not a number',
               collector=cid, metric=metric.name, labels=labels, families=[metric])
        return

    escaped_labels: Dict[str, str] = {}
    for label_name in labels:
        escaped_label = openmetrics.escape_label_name(label_name, escaping)
        previous = escaped_labels.get(escaped_label)
        if previous is not None and previous != label_name:
            report(f'label names {previous!r} and {label_name!r} collide after escaping as {escaped_label!r}',
                   collector=cid, metric=metric.name, labels=labels, families=[metric])
        else:
            escaped_labels[escaped_label] = label_name

    key = (name, _label_key(labels))
    previous = seen_samples.get(key)
    if previous is not None:
        other_cid, other = previous
        related = [metric] if other is metric else [metric, other]
        report(f'duplicate sample {name!r} with identical labels',
               collector=cid, metric=metric.name, labels=labels, families=related)
    else:
        seen_samples[key] = (cid, metric)

    escaped = openmetrics.escape_metric_name(name, escaping)
    previous = escaped_samples.get(escaped)
    if previous is not None and previous[0] != name:
        report(f'sample names {previous[0]!r} and {name!r} collide after escaping as {escaped!r}',
               collector=cid, metric=metric.name, labels=labels, families=[metric, previous[2]])
    else:
        escaped_samples.setdefault(escaped, (name, cid, metric))


def _validate_histogram(cid: str, metric: Metric, report) -> None:
    name = metric.name
    count_name = name + ('_count' if metric.type == 'histogram' else '_gcount')
    buckets: Dict[frozenset, List[tuple]] = {}
    counts: Dict[frozenset, object] = {}
    for sample in metric.samples:
        try:
            sample_name, labels = sample.name, sample.labels
        except AttributeError:
            continue  # Already reported by _validate_sample.
        if not isinstance(labels, dict):
            continue
        if sample_name == name + '_bucket':
            le = labels.get('le')
            if le is None:
                report('histogram bucket sample is missing the "le" label',
                       collector=cid, metric=name, labels=labels, families=[metric])
                continue
            try:
                le_value = float(le)
            except (TypeError, ValueError):
                report(f'histogram bucket boundary {le!r} is not a number',
                       collector=cid, metric=name, labels=labels, families=[metric])
                continue
            if le_value != le_value:
                report(f'histogram bucket boundary {le!r} is NaN',
                       collector=cid, metric=name, labels=labels, families=[metric])
                continue
            buckets.setdefault(_label_key(labels, without='le'), []).append((le_value, sample))
        else:
            if 'le' in labels:
                report(f'sample {sample_name!r} of histogram family has an "le" label but is not a bucket',
                       collector=cid, metric=name, labels=labels, families=[metric])
            if sample_name == count_name:
                counts[_label_key(labels)] = sample

    for key, entries in buckets.items():
        labels = entries[0][1].labels
        entries.sort(key=lambda entry: entry[0])
        boundaries = [entry[0] for entry in entries]
        if any(upper <= lower for lower, upper in zip(boundaries, boundaries[1:])):
            report('histogram bucket boundaries are not strictly increasing',
                   collector=cid, metric=name, labels=labels, families=[metric])
        if boundaries[-1] != float('inf'):
            report('histogram is missing the +Inf bucket',
                   collector=cid, metric=name, labels=labels, families=[metric])
        try:
            cumulative = [float(entry[1].value) for entry in entries]
        except (TypeError, ValueError):
            continue  # Already reported by _validate_sample.
        if any(upper < lower for lower, upper in zip(cumulative, cumulative[1:])):
            report('histogram bucket counts are not non-decreasing',
                   collector=cid, metric=name, labels=labels, families=[metric])
        count_sample = counts.get(key)
        if count_sample is not None and boundaries[-1] == float('inf'):
            try:
                count_value = float(count_sample.value)
            except (TypeError, ValueError):
                continue  # Already reported by _validate_sample.
            if count_value != cumulative[-1]:
                report(f'histogram count {count_value} does not match the +Inf bucket count {cumulative[-1]}',
                       collector=cid, metric=name, labels=labels, families=[metric])


def _validate_summary(cid: str, metric: Metric, report) -> None:
    name = metric.name
    for sample in metric.samples:
        try:
            sample_name, labels = sample.name, sample.labels
        except AttributeError:
            continue  # Already reported by _validate_sample.
        if not isinstance(labels, dict):
            continue
        if sample_name == name:
            quantile = labels.get('quantile')
            if quantile is None:
                report('summary sample is missing the "quantile" label',
                       collector=cid, metric=name, labels=labels, families=[metric])
                continue
            try:
                quantile_value = float(quantile)
            except (TypeError, ValueError):
                report(f'summary quantile {quantile!r} is not a number',
                       collector=cid, metric=name, labels=labels, families=[metric])
                continue
            if not 0 <= quantile_value <= 1:
                report(f'summary quantile {quantile!r} is outside the range [0, 1]',
                       collector=cid, metric=name, labels=labels, families=[metric])
        elif 'quantile' in labels:
            report(f'sample {sample_name!r} of summary family has a "quantile" label',
                   collector=cid, metric=name, labels=labels, families=[metric])
