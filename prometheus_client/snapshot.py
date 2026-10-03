"""Strict, validated snapshot export.

Unlike the regular exposition path -- which streams metrics straight from the
collectors into the encoder and therefore often surfaces a problem only as an
invalid scrape -- :func:`collect_snapshot` first gathers *all* metrics from
each collector exactly once, copies them into immutable snapshots and runs
explicit consistency checks before handing them to an existing encoder.

Callers decide how to react to problems:

* :data:`RAISE`: any failure fails the whole scrape (the default),
* :data:`ISOLATE`: offending collectors/metric families are dropped and the
  problems are reported on :class:`MetricSnapshot.errors`.

Validation performed:

* metric family name/type conflicts,
* duplicate samples with the same name and label set,
* label set (shape) and escaped label-name conflicts,
* histogram bucket-boundary monotonicity, required ``+Inf`` bucket and
  ``_count`` consistency (gauge histograms included),
* summary quantile range (``0 <= quantile <= 1``),
* collisions that only appear after name escaping,
* non-finite, non-numeric or missing sample values are rejected, while the
  legal special float values (``NaN``/``+Inf``/``-Inf``) are preserved.

The input samples are never modified; the existing fast exposition interfaces
are unchanged.
"""

from dataclasses import dataclass, field
import math
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from .metrics_core import METRIC_TYPES, Metric
from .openmetrics import exposition as _openmetrics
from .samples import Sample

RAISE = 'raise'
ISOLATE = 'isolate'

ESCAPING = _openmetrics.UNDERSCORES

_PLUS_INF = ('+Inf', '+inf', 'Inf', 'inf', 'Infinity', 'infinity')


class SnapshotError(Exception):
    """Base class for strict snapshot problems.

    Every concrete error identifies the :attr:`collector` it came from (the
    collector object, or ``None`` for registry-provided data such as
    ``target_info``), the metric :attr:`family` and the offending
    :attr:`sample` (if applicable).
    """

    def __init__(
        self,
        message: str,
        collector: Any = None,
        family: Optional[str] = None,
        sample: Optional["SampleIdentity"] = None,
    ) -> None:
        super().__init__(message)
        self.collector = collector
        self.family = family
        self.sample = sample


class CollectionError(SnapshotError):
    """A collector raised while being collected (or returned bad data)."""

    def __init__(self, collector: Any, cause: BaseException) -> None:
        super().__init__(
            'collector {0} failed during collection: {1}: {2}'.format(
                collector_name(collector), type(cause).__name__, cause),
            collector=collector)
        self.__cause__ = cause


class ValidationError(SnapshotError):
    """A metric family failed structural validation."""


class FamilyNameError(ValidationError):
    """A metric family has an invalid name or unknown type."""


class TypeConflictError(ValidationError):
    """Families exposed under the same name declare different types."""

    def __init__(self, identity: "FamilyIdentity", existing: "FamilyIdentity",
                 collector: Any) -> None:
        super().__init__(
            'metric family {name!r} has conflicting types {new!r} and '
            '{old!r}'.format(
                name=identity.name, new=identity.type, old=existing.type),
            collector=collector,
            family=identity.name,
            sample=None)
        self.existing = existing


class EscapedNameCollisionError(ValidationError):
    """Two distinct names become identical after escaping."""

    def __init__(self, escaped: str, original: str, existing: str,
                 collector: Any, family: Optional[str] = None) -> None:
        super().__init__(
            'escaped name collision: {a!r} and {b!r} both escape to '
            '{escaped!r}'.format(a=existing, b=original, escaped=escaped),
            collector=collector,
            family=family)
        self.escaped_name = escaped
        self.existing_name = existing


class DuplicateSampleError(ValidationError):
    """The same sample (name + label set) occurs more than once."""


class LabelSetMismatchError(ValidationError):
    """Samples sharing a name use different sets of label names."""


class LabelNameCollisionError(ValidationError):
    """Two distinct label names become identical after escaping."""


class HistogramError(ValidationError):
    """Histogram buckets or count are inconsistent."""


class SummaryError(ValidationError):
    """A summary quantile is out of range."""


class InvalidSampleValueError(ValidationError):
    """A sample value is not a finite/special numeric value."""


class InvalidSnapshotError(Exception):
    """One or more errors occurred while building a strict snapshot.

    Raised in :data:`RAISE` mode; :attr:`errors` holds every collected
    :class:`SnapshotError` so callers can inspect the full scrape.
    """

    def __init__(self, errors: Sequence[SnapshotError]) -> None:
        self.errors: List[SnapshotError] = list(errors)
        detail = '; '.join(str(e) for e in self.errors)
        super().__init__(
            'strict metric snapshot has {0} error(s): {1}'.format(
                len(self.errors), detail))


def collector_name(collector: Any) -> str:
    """Return a human-readable identity for a collector instance."""

    if collector is None:
        return '<target_info>'
    name = type(collector).__name__
    if name == 'function':
        return repr(collector)
    return '{0}@{1}'.format(name, hex(id(collector)))


@dataclass(frozen=True)
class SampleIdentity:
    """Identity of a sample (its name and label set)."""

    name: str
    labels: Tuple[Tuple[str, str], ...]

    @classmethod
    def of(cls, name: str, labels: Dict[str, str]) -> "SampleIdentity":
        return cls(name=name, labels=tuple(sorted(labels.items())))

    def format_labels(self) -> str:
        return '{' + ','.join('{0}="{1}"'.format(k, v) for k, v in self.labels) + '}'

    def __str__(self) -> str:
        if self.labels:
            return '{0}{1}'.format(self.name, self.format_labels())
        return self.name


@dataclass(frozen=True)
class FamilyIdentity:
    """Identity of a metric family (its name and type)."""

    name: str
    type: str

    def __str__(self) -> str:
        return '{0} ({1})'.format(self.name, self.type)


@dataclass
class FrozenMetric:
    """An immutable copy of a :class:`~prometheus_client.metrics_core.Metric`.

    It exposes the subset of attributes the existing encoders rely on
    (``name``, ``documentation``, ``type``, ``unit`` and ``samples``), so a
    snapshot can be passed to ``generate_latest`` directly.
    """

    name: str
    documentation: str
    type: str
    unit: str
    samples: Tuple[Sample, ...]
    identity: FamilyIdentity
    collector: Any = None


@dataclass
class MetricSnapshot:
    """Result of a validated scrape.

    ``metrics`` holds the accepted :class:`FrozenMetric` families in scrape
    order (same-named, same-type families are merged so each family is encoded
    once). ``errors`` holds every problem found; in :data:`ISOLATE` mode
    offending families are simply absent from ``metrics``.
    """

    metrics: List[FrozenMetric]
    errors: List[SnapshotError] = field(default_factory=list)
    escaping: str = ESCAPING
    on_error: str = RAISE

    def collect(self) -> Iterable[FrozenMetric]:
        """Yield the accepted frozen metrics (collector protocol)."""

        for metric in self.metrics:
            yield metric

    def raise_if_errors(self) -> None:
        """Raise :class:`InvalidSnapshotError` if any error was recorded."""

        if self.errors:
            raise InvalidSnapshotError(self.errors)


def _is_number(value: Any) -> bool:
    # bool is a subclass of int but an invalid sample value on its own.
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        # NaN and +/-Inf are legal exposition values; only NaN with a
        # problematic payload is rejected, never via a blanket isfinite check.
        return math.isfinite(value) or math.isnan(value) or math.isinf(value)
    return False


def _escape_metric_name(name: str, escaping: str) -> str:
    return _openmetrics.escape_metric_name(name, escaping)


def _escape_label_name(name: str, escaping: str) -> str:
    return _openmetrics.escape_label_name(name, escaping)


def _freeze_sample(sample: Sample) -> Sample:
    return Sample(
        name=sample.name,
        labels=dict(sample.labels),
        value=sample.value,
        timestamp=sample.timestamp,
        exemplar=sample.exemplar,
        native_histogram=sample.native_histogram,
    )


def _freeze_metric(metric: Metric, collector: Any) -> FrozenMetric:
    return FrozenMetric(
        name=metric.name,
        documentation=metric.documentation,
        type=metric.type,
        unit=metric.unit,
        samples=tuple(_freeze_sample(s) for s in metric.samples),
        identity=FamilyIdentity(metric.name, metric.type),
        collector=collector,
    )


def _verify_family(
    metric: FrozenMetric,
    escaping: str,
) -> Tuple[Optional[SnapshotError], Optional[SnapshotError]]:
    """Validate everything that can be checked within one family.

    Returns ``(family_error, first_sample_error)``: ``family_error`` (if set)
    disqualifies the whole family; ``first_sample_error`` is set when the
    family must still be isolated but the problem is sample specific.
    """

    family_error: Optional[SnapshotError] = None
    if not isinstance(metric.name, str) or not metric.name:
        family_error = FamilyNameError(
            'metric family has an empty or non-string name',
            collector=metric.collector)
    elif metric.type not in METRIC_TYPES:
        family_error = FamilyNameError(
            'metric family {0!r} has invalid type {1!r}'.format(
                metric.name, metric.type),
            collector=metric.collector, family=metric.name)
    if family_error is not None:
        return family_error, family_error

    sample_error: Optional[SnapshotError] = None

    for sample in metric.samples:
        sample_identity = SampleIdentity.of(sample.name, sample.labels)

        if not _is_number(sample.value):
            sample_error = InvalidSampleValueError(
                'sample {0} of family {1!r} from collector {2} has invalid '
                'value {3!r}'.format(
                    sample_identity, metric.name,
                    collector_name(metric.collector), sample.value),
                collector=metric.collector, family=metric.name,
                sample=sample_identity)
            break

        raw_label_names = [name for name, _ in sample_identity.labels]
        if len(set(raw_label_names)) != len(raw_label_names):
            sample_error = LabelNameCollisionError(
                'sample {0} of family {1!r} from collector {2} contains '
                'duplicate label names'.format(
                    sample_identity, metric.name,
                    collector_name(metric.collector)),
                collector=metric.collector, family=metric.name,
                sample=sample_identity)
            break

        escaped_label_names = [
            _escape_label_name(name, escaping) for name in raw_label_names]
        if len(set(escaped_label_names)) != len(escaped_label_names):
            raw = sorted(set(raw_label_names))
            collisions: Dict[str, List[str]] = {}
            for name in raw:
                collisions.setdefault(
                    _escape_label_name(name, escaping), []).append(name)
            joined = ', '.join(
                '{0} (from {1})'.format(escaped, sorted(originals))
                for escaped, originals in collisions.items()
                if len(originals) > 1)
            sample_error = LabelNameCollisionError(
                'sample {0} of family {1!r} from collector {2} has label '
                'names colliding after escaping: {3}'.format(
                    sample_identity, metric.name,
                    collector_name(metric.collector), joined),
                collector=metric.collector, family=metric.name,
                sample=sample_identity)
            break

        if not isinstance(sample.name, str) or not sample.name:
            sample_error = FamilyNameError(
                'family {0!r} from collector {1} contains a sample with an '
                'empty or non-string name'.format(
                    metric.name, collector_name(metric.collector)),
                collector=metric.collector, family=metric.name,
                sample=sample_identity)
            break

        for label_name, label_value in sample.labels.items():
            if not isinstance(label_name, str) or not isinstance(label_value, str):
                sample_error = ValidationError(
                    'sample {0} of family {1!r} from collector {2} has a '
                    'non-string label name or value'.format(
                        sample_identity, metric.name,
                        collector_name(metric.collector)),
                    collector=metric.collector, family=metric.name,
                    sample=sample_identity)
                break
        if sample_error is not None:
            break

    if sample_error is None:
        if metric.type in ('histogram', 'gaugehistogram'):
            sample_error = _verify_histogram(metric)
        elif metric.type == 'summary':
            sample_error = _verify_summary(metric)

    return None, sample_error


def _parse_bound(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _numeric_less_equal(a: float, b: float) -> bool:
    """NaN-aware ``a <= b`` for cumulative counts: NaN only equals NaN."""

    if math.isnan(a) and math.isnan(b):
        return True
    return a <= b


def _verify_histogram(metric: FrozenMetric) -> Optional[SnapshotError]:
    bucket_suffix = '_bucket'
    count_name = metric.name + (
        '_gcount' if metric.type == 'gaugehistogram' else '_count')
    bucket_samples = [s for s in metric.samples
                      if s.name == metric.name + bucket_suffix]

    if not bucket_samples:
        return HistogramError(
            'histogram family {0!r} from collector {1} has no {2!r} '
            'samples'.format(
                metric.name, collector_name(metric.collector),
                metric.name + bucket_suffix),
            collector=metric.collector, family=metric.name)

    groups: Dict[Tuple[Tuple[str, str], ...], List[Sample]] = {}
    for sample in bucket_samples:
        labels = [(k, v) for k, v in sample.labels.items() if k != 'le']
        groups.setdefault(tuple(sorted(labels)), []).append(sample)

    for group_labels, samples in groups.items():
        boundaries: List[float] = []
        for sample in samples:
            if 'le' not in sample.labels:
                identity = SampleIdentity.of(sample.name, sample.labels)
                return HistogramError(
                    'histogram bucket sample {0} of family {1!r} from '
                    'collector {2} is missing the le label'.format(
                        identity, metric.name,
                        collector_name(metric.collector)),
                    collector=metric.collector, family=metric.name,
                    sample=identity)
            boundary = _parse_bound(sample.labels['le'])
            if boundary is None or math.isnan(boundary):
                identity = SampleIdentity.of(sample.name, sample.labels)
                return HistogramError(
                    'histogram bucket {0} of family {1!r} from collector '
                    '{2} has a non-numeric or NaN le boundary {3!r}'.format(
                        identity, metric.name,
                        collector_name(metric.collector),
                        sample.labels['le']),
                    collector=metric.collector, family=metric.name,
                    sample=identity)
            boundaries.append(boundary)

        for prev, cur in zip(boundaries, boundaries[1:]):
            if not prev < cur:
                identity = SampleIdentity.of(
                    metric.name + bucket_suffix,
                    dict(group_labels + (('le', _boundary_label(
                        samples, boundaries, cur)),)))
                return HistogramError(
                    'histogram family {0!r} from collector {1} has '
                    'non-monotonic bucket boundaries ({2} followed by '
                    '{3})'.format(
                        metric.name, collector_name(metric.collector),
                        prev, cur),
                    collector=metric.collector, family=metric.name,
                    sample=identity)

        last_label = samples[-1].labels['le']
        if last_label not in _PLUS_INF or boundaries[-1] != math.inf:
            identity = SampleIdentity.of(
                samples[-1].name, samples[-1].labels)
            return HistogramError(
                'histogram family {0!r} from collector {1} must have its '
                'last bucket at +Inf, found {2!r}'.format(
                    metric.name, collector_name(metric.collector), last_label),
                collector=metric.collector, family=metric.name,
                sample=identity)

        for prev, cur in zip(samples, samples[1:]):
            if not _numeric_less_equal(
                    float(prev.value), float(cur.value)):
                identity = SampleIdentity.of(cur.name, cur.labels)
                return HistogramError(
                    'histogram family {0!r} from collector {1} has '
                    'non-monotonic cumulative bucket counts ({2} followed '
                    'by {3})'.format(
                        metric.name, collector_name(metric.collector),
                        prev.value, cur.value),
                    collector=metric.collector, family=metric.name,
                    sample=identity)

        if metric.type == 'histogram':
            group_dict = dict(group_labels)
            count_samples = [s for s in metric.samples
                             if s.name == count_name
                             and all(s.labels.get(k) == v
                                     for k, v in group_dict.items())
                             and len(s.labels) == len(group_dict)]
            if count_samples:
                inf_value = float(samples[-1].value)
                count_value = float(count_samples[0].value)
                if not (
                        (math.isnan(inf_value) and math.isnan(count_value))
                        or inf_value == count_value):
                    identity = SampleIdentity.of(
                        count_samples[0].name, count_samples[0].labels)
                    return HistogramError(
                        'histogram family {0!r} from collector {1} has a '
                        '_count value {2} inconsistent with the +Inf bucket '
                        'value {3}'.format(
                            metric.name, collector_name(metric.collector),
                            count_value, inf_value),
                        collector=metric.collector, family=metric.name,
                        sample=identity)

    return None


def _boundary_label(samples: Sequence[Sample], boundaries: Sequence[float],
                    boundary: float) -> str:
    for sample, value in zip(samples, boundaries):
        if value is boundary:
            return sample.labels['le']
    return str(boundary)


def _verify_summary(metric: FrozenMetric) -> Optional[SnapshotError]:
    for sample in metric.samples:
        if 'quantile' not in sample.labels:
            continue
        raw = sample.labels['quantile']
        identity = SampleIdentity.of(sample.name, sample.labels)
        quantile = _parse_bound(raw)
        if quantile is None or math.isnan(quantile) or not (
                0.0 <= quantile <= 1.0):
            return SummaryError(
                'summary family {0!r} from collector {1} has an out of '
                'range quantile {2!r} (must be between 0 and 1)'.format(
                    metric.name, collector_name(metric.collector), raw),
                collector=metric.collector, family=metric.name,
                sample=identity)
    return None


def _iter_collectors(source: Any) -> Iterable[Tuple[Any, Any]]:
    """Yield ``(collector, owner)`` pairs to invoke once.

    For a :class:`~prometheus_client.registry.CollectorRegistry` every
    registered collector is invoked once (collectors without names too);
    registry-provided data such as ``target_info`` is yielded with a
    ``None`` owner. Any other object is treated as a single collector.
    """

    if hasattr(source, '_collector_to_names') and hasattr(source, '_lock'):
        with source._lock:
            collector_map = dict(source._collector_to_names)
            nameless = list(getattr(source, '_collectors_without_names', ()))
            target_info = dict(source._target_info) if source._target_info else None

        if target_info:
            metric = Metric('target', 'Target metadata', 'info')
            metric.add_sample('target_info', target_info, 1)
            yield metric, None

        seen = set()
        for collector in list(collector_map):
            yield collector, collector
            seen.add(id(collector))
        for collector in nameless:
            if id(collector) not in seen:
                yield collector, collector
        return

    yield source, source


def collect_snapshot(
    registry: Any,
    escaping: str = ESCAPING,
    on_error: str = RAISE,
) -> MetricSnapshot:
    """Collect and validate every metric family from ``registry``.

    Each collector's ``collect()`` is invoked exactly once; the returned
    metrics are copied (never mutated) and validated before being returned on
    a :class:`MetricSnapshot`.

    Args:
        registry: a :class:`~prometheus_client.registry.CollectorRegistry` or
            any object exposing ``collect()``.
        escaping: name-escaping scheme used to detect post-escaping collisions;
            defaults to ``underscores``.
        on_error: :data:`RAISE` (default) raises :class:`InvalidSnapshotError`
            containing every problem; :data:`ISOLATE` drops the offending
            collectors/families and reports them on
            :attr:`MetricSnapshot.errors` instead.
    """

    if on_error not in (RAISE, ISOLATE):
        raise ValueError(
            'on_error must be RAISE or ISOLATE, got {0!r}'.format(on_error))

    errors: List[SnapshotError] = []

    # Families keyed by raw name and escaped name, in scrape order.
    by_name: Dict[str, FrozenMetric] = {}
    name_order: List[str] = []
    escaped_family_names: Dict[str, str] = {}
    # Global sample index keyed by escaped sample name + escaped label set.
    sample_index: Dict[
        Tuple[str, Tuple[Tuple[str, str], ...]],
        Tuple[SampleIdentity, FrozenMetric],
    ] = {}
    # Label-name shapes observed per escaped sample name.
    label_shapes: Dict[str, Tuple[Tuple[str, ...], FrozenMetric]] = {}

    for item, owner in _iter_collectors(registry):
        if owner is None:
            # Registry-generated metric (target_info), already built.
            frozen_metrics = [_freeze_metric(item, None)]
        else:
            collector = item
            try:
                collected = list(collector.collect())
            except Exception as cause:
                errors.append(CollectionError(collector, cause))
                continue

            frozen_metrics = []
            bad_collection = False
            for raw_metric in collected:
                try:
                    frozen_metrics.append(
                        _freeze_metric(raw_metric, collector))
                except Exception as cause:
                    errors.append(ValidationError(
                        'collector {0} produced an invalid metric family: '
                        '{1}: {2}'.format(
                            collector_name(collector),
                            type(cause).__name__, cause),
                        collector=collector))
                    bad_collection = True
                    break
            if bad_collection:
                continue

        for frozen in frozen_metrics:
            _, sample_error = _verify_family(frozen, escaping)
            if sample_error is not None:
                errors.append(sample_error)
                continue

            escaped_name = _escape_metric_name(frozen.name, escaping)

            # Same raw family name: types must agree.
            if frozen.name in by_name:
                existing = by_name[frozen.name]
                if existing.type != frozen.type:
                    errors.append(TypeConflictError(
                        frozen.identity, existing.identity, frozen.collector))
                    continue
            else:
                # Escaped family-name collisions across distinct raw names.
                if escaped_name in escaped_family_names:
                    existing_name = escaped_family_names[escaped_name]
                    if existing_name != frozen.name:
                        errors.append(EscapedNameCollisionError(
                            escaped_name, frozen.name, existing_name,
                            frozen.collector, family=frozen.name))
                        continue
                by_name[frozen.name] = frozen
                name_order.append(frozen.name)
                escaped_family_names[escaped_name] = frozen.name

            # Validate the family's samples against the global scrape state.
            batch: List[Tuple[
                Tuple[str, Tuple[Tuple[str, str], ...]],
                Tuple[SampleIdentity, FrozenMetric],
            ]] = []
            batch_keys: set = set()
            batch_shapes: List[Tuple[str, Tuple[str, ...]]] = []
            family_failed = False
            for sample in frozen.samples:
                sample_identity = SampleIdentity.of(
                    sample.name, sample.labels)
                escaped_sample_name = _escape_metric_name(
                    sample.name, escaping)
                escaped_labels = tuple(
                    (_escape_label_name(name, escaping), value)
                    for name, value in sample_identity.labels)
                escaped_label_names = tuple(name for name, _ in escaped_labels)
                escaped_key = (escaped_sample_name, escaped_labels)

                existing_shape = label_shapes.get(escaped_sample_name)
                if existing_shape is not None:
                    existing_names, existing_metric = existing_shape
                    if existing_names != escaped_label_names:
                        errors.append(LabelSetMismatchError(
                            'sample name {0!r} is used with inconsistent '
                            'label sets: {1} (family {2!r}, collector {3}) '
                            'vs {4} (family {5!r}, collector {6})'.format(
                                sample.name,
                                _format_label_names(existing_names),
                                existing_metric.name,
                                collector_name(existing_metric.collector),
                                _format_label_names(escaped_label_names),
                                frozen.name,
                                collector_name(frozen.collector)),
                            collector=frozen.collector, family=frozen.name,
                            sample=sample_identity))
                        family_failed = True
                        break

                previous = (sample_index.get(escaped_key)
                            if escaped_key not in batch_keys else
                            (sample_identity, frozen))
                if previous is not None:
                    previous_identity, previous_metric = previous
                    if (escaped_key in batch_keys
                            or previous_identity == sample_identity):
                        errors.append(DuplicateSampleError(
                            'duplicate sample {0} in family {1!r} from '
                            'collector {2}; already seen in family {3!r} '
                            'from collector {4}'.format(
                                sample_identity, frozen.name,
                                collector_name(frozen.collector),
                                previous_metric.name,
                                collector_name(previous_metric.collector)),
                            collector=frozen.collector, family=frozen.name,
                            sample=sample_identity))
                    else:
                        errors.append(EscapedNameCollisionError(
                            escaped_sample_name, sample_identity.name,
                            previous_identity.name, frozen.collector,
                            family=frozen.name))
                    family_failed = True
                    break

                batch.append((escaped_key, (sample_identity, frozen)))
                batch_keys.add(escaped_key)
                batch_shapes.append((
                    escaped_sample_name,
                    escaped_label_names))

            if family_failed:
                # Families that passed internal checks but clash with the rest
                # of the scrape are isolated wholesale; their first appearance
                # (already committed) wins.
                continue

            for key, value in batch:
                sample_index[key] = value
            for escaped_sample_name, shape in batch_shapes:
                if escaped_sample_name not in label_shapes:
                    label_shapes[escaped_sample_name] = shape, frozen

    # Merge same-name, same-type families so each family is encoded once.
    merged: List[FrozenMetric] = []
    merged_samples: Dict[str, List[Sample]] = {}
    for name in name_order:
        family = by_name[name]
        samples = merged_samples.setdefault(name, [])
        samples.extend(family.samples)
    for name in name_order:
        family = by_name[name]
        merged.append(FrozenMetric(
            name=family.name,
            documentation=family.documentation,
            type=family.type,
            unit=family.unit,
            samples=tuple(merged_samples[name]),
            identity=family.identity,
            collector=family.collector,
        ))

    snapshot = MetricSnapshot(
        metrics=merged, errors=errors, escaping=escaping, on_error=on_error)
    if on_error == RAISE:
        snapshot.raise_if_errors()
    return snapshot


def _format_label_names(names: Tuple[str, ...]) -> str:
    return '[' + ','.join(names) + ']'


class StrictRegistry:
    """Collector wrapper that produces a validated snapshot on ``collect()``.

    This is the explicit strict export entry point usable with the existing
    encoders, e.g. ``generate_latest(StrictRegistry(REGISTRY))``.
    """

    def __init__(
        self,
        registry: Any,
        escaping: str = ESCAPING,
        on_error: str = RAISE,
    ) -> None:
        self.registry = registry
        self.escaping = escaping
        self.on_error = on_error

    def collect(self) -> Iterable[FrozenMetric]:
        return collect_snapshot(
            self.registry,
            escaping=self.escaping,
            on_error=self.on_error).collect()

    def snapshot(self) -> MetricSnapshot:
        """Build the :class:`MetricSnapshot` without encoding it."""

        return collect_snapshot(
            self.registry,
            escaping=self.escaping,
            on_error=self.on_error)


def generate_latest_strict(
    registry: Any,
    escaping: str = ESCAPING,
    on_error: str = RAISE,
    fmt: str = 'text',
    version: str = '1.0.0',
) -> bytes:
    """Encode a validated strict snapshot using an existing encoder.

    Args:
        registry: registry or collector to snapshot.
        escaping: name-escaping scheme.
        on_error: :data:`RAISE` or :data:`ISOLATE`.
        fmt: ``'text'`` for the Prometheus text format (default) or
            ``'openmetrics'`` for the OpenMetrics text format.
        version: OpenMetrics version when ``fmt='openmetrics'``.
    """

    snapshot = collect_snapshot(
        registry, escaping=escaping, on_error=on_error)
    if fmt == 'text':
        from .exposition import generate_latest
        return generate_latest(snapshot, escaping=escaping)
    if fmt == 'openmetrics':
        return _openmetrics.generate_latest(
            snapshot, escaping=escaping, version=version)
    raise ValueError(
        "fmt must be 'text' or 'openmetrics', got {0!r}".format(fmt))
