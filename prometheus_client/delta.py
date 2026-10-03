"""In-memory collector that rebuilds cumulative metrics from delta batches.

An agent reports per-interval *deltas* (how much a counter increased, or the
non-cumulative bucket counts of a histogram, within one interval).  Prometheus
however needs cumulative values.  This module provides :class:`DeltaCollector`,
a purely in-memory bridge that accepts ordered batches of deltas and exposes
the reconstructed cumulative series through the usual collector interface.

Batching rules (per stream):

* A batch carries a stream identifier, a generation, a half-open interval
  ``[interval_start, interval_end)`` and a monotonically increasing sequence
  number.
* Re-applying the most recent batch with identical content is an idempotent
  no-op; the same sequence number with different content is rejected.
* Sequences must advance exactly by one.  Older sequences and gaps are
  rejected, as are overlapping or non-contiguous intervals.
* A higher generation resets the stream: cumulative state is rebuilt from
  scratch and ``created`` timestamps move to the new interval.  Late batches
  from an older generation are rejected and never touch the new stream.
* Histogram deltas carry non-cumulative buckets whose counts must add up to
  the reported total count.  The bucket layout may only change together with
  the generation.
* A batch is validated as a whole and committed atomically; ``collect()``
  always observes either the pre-commit or the post-commit snapshot.
* The number of tracked series is bounded.  Least-recently-used series are
  evicted, every eviction is reported, and a reappearing series restarts its
  cumulative value instead of silently pretending continuity.
"""

import math
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Tuple, Union

from .metrics_core import CounterMetricFamily, Metric
from .utils import floatToGoString
from .validation import _validate_metric_name

Labels = Mapping[str, str]
_LabelsTuple = Tuple[Tuple[str, str], ...]
_SeriesKey = Tuple[str, str, _LabelsTuple]  # (stream, metric name, labels)


class DeltaError(ValueError):
    """Base class for all delta batch rejections."""


class InvalidDeltaError(DeltaError):
    """The batch content itself is malformed."""


class DuplicateSeriesError(InvalidDeltaError):
    """The same series (name + labels) appears twice in one batch."""


class HistogramValidationError(InvalidDeltaError):
    """A histogram delta is inconsistent (bad buckets or count)."""


class BucketLayoutChangedError(InvalidDeltaError):
    """Bucket boundaries changed without a generation change."""


class StaleGenerationError(DeltaError):
    """A batch from an older generation arrived after a newer one."""


class StaleSequenceError(DeltaError):
    """The sequence number was already committed and is not the replayable
    most recent batch."""


class SequenceGapError(DeltaError):
    """The sequence number skipped ahead; intermediate batches are missing."""


class BatchConflictError(DeltaError):
    """The sequence number matches the last committed batch but the content
    differs."""


class IntervalOverlapError(DeltaError):
    """The batch interval overlaps the already committed interval."""


class IntervalGapError(DeltaError):
    """The batch interval does not start where the previous one ended."""


@dataclass(frozen=True)
class CounterDelta:
    """Delta of a single counter series within one interval."""

    name: str
    labels: Labels
    value: float


@dataclass(frozen=True)
class HistogramDelta:
    """Delta of a single histogram series within one interval.

    ``buckets`` are non-cumulative ``(upper_bound, count)`` pairs whose
    counts must sum to ``count``.
    """

    name: str
    labels: Labels
    buckets: Tuple[Tuple[float, float], ...]
    count: float
    sum: float


SeriesDelta = Union[CounterDelta, HistogramDelta]


@dataclass(frozen=True)
class DeltaBatch:
    """One atomic batch of deltas reported by an agent."""

    stream: str
    generation: int
    sequence: int
    interval_start: float
    interval_end: float
    series: Tuple[SeriesDelta, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Eviction:
    """Record of one evicted series."""

    stream: str
    name: str
    labels: _LabelsTuple
    reason: str


@dataclass
class _StreamState:
    generation: int
    last_sequence: int
    last_interval_end: float
    last_content: tuple


@dataclass
class _CounterState:
    value: float
    created: float


@dataclass
class _HistogramState:
    bounds: Tuple[float, ...]
    counts: List[float]  # non-cumulative, aligned with bounds
    count: float
    sum: float
    created: float


def _labels_tuple(labels: Labels) -> _LabelsTuple:
    if not isinstance(labels, Mapping):
        raise InvalidDeltaError('labels must be a mapping of strings')
    items = []
    for key, value in labels.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise InvalidDeltaError('label names and values must be strings')
        items.append((key, value))
    return tuple(sorted(items))


def _canonical_series(series: SeriesDelta) -> tuple:
    labels = _labels_tuple(series.labels)
    if isinstance(series, CounterDelta):
        return ('counter', series.name, labels, float(series.value))
    buckets = tuple((float(bound), float(count)) for bound, count in series.buckets)
    return ('histogram', series.name, labels, buckets, float(series.count), float(series.sum))


def _canonical_content(batch: DeltaBatch) -> tuple:
    entries = tuple(sorted(_canonical_series(s) for s in batch.series))
    return (
        batch.stream,
        batch.generation,
        batch.sequence,
        float(batch.interval_start),
        float(batch.interval_end),
        entries,
    )


class DeltaCollector:
    """Rebuilds cumulative metrics from per-interval delta batches.

    Purely in-memory; register it on a :class:`CollectorRegistry` like any
    other collector.
    """

    def __init__(self, max_series: int = 10000, eviction_log_size: int = 128):
        if max_series < 1:
            raise ValueError('max_series must be positive')
        self._max_series = max_series
        self._lock = threading.Lock()
        self._streams: Dict[str, _StreamState] = {}
        # Ordered by least-recently-committed first, for bounded eviction.
        self._series: 'OrderedDict[_SeriesKey, Union[_CounterState, _HistogramState]]' = OrderedDict()
        # Metric name -> (kind, label names) to keep exposition consistent.
        self._meta: Dict[str, Tuple[str, Tuple[str, ...]]] = {}
        self._evictions_total = 0
        self._eviction_log_size = eviction_log_size
        self._evictions: List[Eviction] = []

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def apply_batch(self, batch: DeltaBatch) -> str:
        """Validate and commit one batch.

        Returns ``'committed'`` when the batch was applied and ``'duplicate'``
        when it is an identical replay of the most recent batch.  Raises a
        :class:`DeltaError` subclass otherwise; a rejected batch never
        modifies any state.
        """
        content = _canonical_content(batch)
        self._validate_batch(batch)
        with self._lock:
            stream = self._streams.get(batch.stream)
            if stream is not None and batch.generation < stream.generation:
                raise StaleGenerationError(
                    f'stream {batch.stream!r} is at generation {stream.generation}, '
                    f'rejecting batch from generation {batch.generation}')
            new_generation = stream is None or batch.generation > stream.generation
            if not new_generation:
                if self._is_duplicate(stream, batch, content):
                    return 'duplicate'
            self._validate_series(batch, new_generation)
            self._commit(batch, content)
            return 'committed'

    @staticmethod
    def _is_duplicate(stream: _StreamState, batch: DeltaBatch, content: tuple) -> bool:
        if batch.sequence == stream.last_sequence:
            if content == stream.last_content:
                return True
            raise BatchConflictError(
                f'stream {batch.stream!r} sequence {batch.sequence} was already '
                'committed with different content')
        if batch.sequence < stream.last_sequence:
            raise StaleSequenceError(
                f'stream {batch.stream!r} already committed sequence '
                f'{stream.last_sequence}, rejecting {batch.sequence}')
        if batch.sequence > stream.last_sequence + 1:
            raise SequenceGapError(
                f'stream {batch.stream!r} expects sequence '
                f'{stream.last_sequence + 1}, got {batch.sequence}')
        if batch.interval_start < stream.last_interval_end:
            raise IntervalOverlapError(
                f'stream {batch.stream!r} interval starts at '
                f'{batch.interval_start} before previous interval end '
                f'{stream.last_interval_end}')
        if batch.interval_start > stream.last_interval_end:
            raise IntervalGapError(
                f'stream {batch.stream!r} interval starts at '
                f'{batch.interval_start} after previous interval end '
                f'{stream.last_interval_end}')
        return False

    @staticmethod
    def _validate_batch(batch: DeltaBatch) -> None:
        if not batch.stream:
            raise InvalidDeltaError('stream identifier must not be empty')
        if not isinstance(batch.generation, int) or isinstance(batch.generation, bool) \
                or batch.generation < 0:
            raise InvalidDeltaError('generation must be a non-negative integer')
        if not isinstance(batch.sequence, int) or isinstance(batch.sequence, bool) \
                or batch.sequence < 0:
            raise InvalidDeltaError('sequence must be a non-negative integer')
        if not (math.isfinite(batch.interval_start) and math.isfinite(batch.interval_end)):
            raise InvalidDeltaError('interval bounds must be finite')
        if not batch.interval_end > batch.interval_start:
            raise InvalidDeltaError('interval_end must be greater than interval_start')

    def _validate_series(self, batch: DeltaBatch, new_generation: bool) -> None:
        seen = set()
        for series in batch.series:
            _validate_metric_name(series.name)
            labels = _labels_tuple(series.labels)
            key = (batch.stream, series.name, labels)
            if key in seen:
                raise DuplicateSeriesError(
                    f'series {series.name!r}{dict(labels)} appears twice in one batch')
            seen.add(key)
            kind = 'counter' if isinstance(series, CounterDelta) else 'histogram'
            labelnames = tuple(name for name, _ in labels)
            meta = self._meta.get(series.name)
            if meta is not None and meta != (kind, labelnames):
                raise InvalidDeltaError(
                    f'metric {series.name!r} was already registered as {meta[0]} '
                    f'with labels {meta[1]}, got {kind} with labels {labelnames}')
            if isinstance(series, CounterDelta):
                self._validate_counter(series)
            else:
                self._validate_histogram(key, series, new_generation)

    @staticmethod
    def _validate_counter(series: CounterDelta) -> None:
        if not math.isfinite(series.value):
            raise InvalidDeltaError(f'counter {series.name!r} delta must be finite')
        if series.value < 0:
            raise InvalidDeltaError(f'counter {series.name!r} delta must not be negative')

    def _validate_histogram(self, key: _SeriesKey, series: HistogramDelta,
                            new_generation: bool) -> None:
        bounds = []
        total = 0.0
        previous = -math.inf
        for bound, count in series.buckets:
            if not math.isfinite(bound):
                raise HistogramValidationError(
                    f'histogram {series.name!r} bucket bounds must be finite')
            if bound <= previous:
                raise HistogramValidationError(
                    f'histogram {series.name!r} bucket bounds must be strictly increasing')
            previous = bound
            if not math.isfinite(count) or count < 0:
                raise HistogramValidationError(
                    f'histogram {series.name!r} bucket counts must be finite and non-negative')
            bounds.append(float(bound))
            total += count
        if not math.isfinite(series.count) or series.count < 0:
            raise HistogramValidationError(
                f'histogram {series.name!r} count must be finite and non-negative')
        if total != series.count:
            raise HistogramValidationError(
                f'histogram {series.name!r} bucket counts sum to {total} '
                f'but count is {series.count}')
        if math.isnan(series.sum):
            raise HistogramValidationError(f'histogram {series.name!r} sum must not be NaN')
        # A new generation discards the old state at commit time, so its
        # bucket layout is no constraint.
        existing = None if new_generation else self._series.get(key)
        if isinstance(existing, _HistogramState) and existing.bounds != tuple(bounds):
            raise BucketLayoutChangedError(
                f'histogram {series.name!r} bucket layout changed from '
                f'{existing.bounds} to {tuple(bounds)} without a generation change')

    def _commit(self, batch: DeltaBatch, content: tuple) -> None:
        stream = self._streams.get(batch.stream)
        if stream is not None and batch.generation > stream.generation:
            # Explicit new generation: drop all state of the old generation
            # so late data can never leak into the new stream.
            for key in [k for k in self._series if k[0] == batch.stream]:
                del self._series[key]
        created = float(batch.interval_start)
        for series in batch.series:
            labels = _labels_tuple(series.labels)
            key = (batch.stream, series.name, labels)
            self._meta.setdefault(
                series.name,
                ('counter' if isinstance(series, CounterDelta) else 'histogram',
                 tuple(name for name, _ in labels)))
            if isinstance(series, CounterDelta):
                state = self._series.get(key)
                if not isinstance(state, _CounterState):
                    state = _CounterState(value=0.0, created=created)
                    self._series[key] = state
                state.value += series.value
            else:
                state = self._series.get(key)
                if not isinstance(state, _HistogramState):
                    state = _HistogramState(
                        bounds=tuple(float(b) for b, _ in series.buckets),
                        counts=[0.0] * len(series.buckets),
                        count=0.0,
                        sum=0.0,
                        created=created)
                    self._series[key] = state
                for i, (_, count) in enumerate(series.buckets):
                    state.counts[i] += count
                state.count += series.count
                state.sum += series.sum
            self._series.move_to_end(key)
        self._streams[batch.stream] = _StreamState(
            generation=batch.generation,
            last_sequence=batch.sequence,
            last_interval_end=float(batch.interval_end),
            last_content=content)
        self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        while len(self._series) > self._max_series:
            (stream, name, labels), _ = self._series.popitem(last=False)
            self._record_eviction(Eviction(stream, name, labels, 'max_series exceeded'))

    def _record_eviction(self, eviction: Eviction) -> None:
        self._evictions_total += 1
        self._evictions.append(eviction)
        if len(self._evictions) > self._eviction_log_size:
            del self._evictions[:len(self._evictions) - self._eviction_log_size]

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    @property
    def evictions_total(self) -> int:
        """Total number of series evicted since this collector was created."""
        with self._lock:
            return self._evictions_total

    def recent_evictions(self) -> Tuple[Eviction, ...]:
        """The most recent evictions, oldest first (bounded log)."""
        with self._lock:
            return tuple(self._evictions)

    def collect(self) -> Iterable[Metric]:
        with self._lock:
            counters: Dict[str, CounterMetricFamily] = {}
            histograms: Dict[str, Metric] = {}
            for (stream, name, labels), state in self._series.items():
                kind, labelnames = self._meta[name]
                label_dict = dict(labels)
                if kind == 'counter':
                    assert isinstance(state, _CounterState)
                    family = counters.get(name)
                    if family is None:
                        family = CounterMetricFamily(
                            name, 'Cumulative counter rebuilt from delta batches.',
                            labels=list(labelnames))
                        counters[name] = family
                    family.add_metric(
                        [label_dict[label] for label in labelnames],
                        state.value, created=state.created)
                else:
                    assert isinstance(state, _HistogramState)
                    metric = histograms.get(name)
                    if metric is None:
                        metric = Metric(
                            name, 'Cumulative histogram rebuilt from delta batches.',
                            'histogram')
                        histograms[name] = metric
                    cumulative = 0.0
                    for bound, count in zip(state.bounds, state.counts):
                        cumulative += count
                        metric.add_sample(
                            name + '_bucket',
                            dict(label_dict, le=floatToGoString(bound)),
                            cumulative)
                    metric.add_sample(
                        name + '_bucket', dict(label_dict, le='+Inf'), state.count)
                    metric.add_sample(name + '_sum', label_dict, state.sum)
                    metric.add_sample(name + '_count', label_dict, state.count)
                    metric.add_sample(name + '_created', label_dict, state.created)
            families: List[Metric] = list(counters.values()) + list(histograms.values())
            internal = Metric(
                'delta_collector', 'Internal state of the delta collector.', 'gauge')
            internal.add_sample(
                'delta_collector_series', {}, float(len(self._series)))
            internal.add_sample(
                'delta_collector_streams', {}, float(len(self._streams)))
            internal.add_sample(
                'delta_collector_evictions_total', {}, float(self._evictions_total))
            families.append(internal)
            return families
