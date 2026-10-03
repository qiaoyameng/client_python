"""In-memory collector that turns interval deltas into cumulative metrics.

Some agents report counter/histogram increments for a reporting interval
every few seconds, while Prometheus expects cumulative values. DeltaCollector
accepts those delta reports as batches and reconstructs the cumulative view
exposed by ``collect()``.

Each batch carries a stream identity, a generation, the interval it covers
and a batch sequence number. The handling rules are:

* Idempotent replay: re-applying the most recent batch of a stream with
  identical content is a no-op. The same sequence number with different
  content is rejected with :class:`BatchConflictError`.
* Ordering: within a generation, batches must arrive in sequence order with
  contiguous intervals. Older sequence numbers, sequence gaps, overlapping
  intervals and interval gaps are all rejected with a specific error instead
  of being silently repaired.
* Generations: a batch with a higher generation resets the stream. The
  cumulative state is rebuilt from scratch and the ``created`` timestamp is
  set to the new interval start. Batches from an older generation arriving
  late are rejected with :class:`StaleGenerationError` and never touch the
  new stream.
* Histograms: buckets are reported as non-cumulative per-interval counts and
  are validated against the reported total count. The bucket layout is fixed
  for the lifetime of a generation; changing it requires a new generation.
* Atomicity: a batch is validated as a whole before anything is committed.
  ``collect()`` holds the same lock, so it always observes either the
  pre-commit or the post-commit snapshot, never a partially applied batch.
* Bounded label recycling: each stream keeps at most
  ``max_series_per_stream`` label sets. The least recently used series is
  evicted when the bound is exceeded. Evictions are reported through
  ``evictions``/``evictions_total`` and the
  ``delta_collector_series_evictions_total`` self-metric. A series that
  reappears after eviction starts over with a fresh ``created`` timestamp
  instead of silently fabricating a continuous cumulative history.

The first batch ever seen for a stream (including after a collector restart,
since state is purely in memory) bootstraps the stream from whatever
sequence number and interval it carries.
"""

from collections import OrderedDict, deque
from dataclasses import dataclass, field
import math
from threading import Lock
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from .metrics_core import Metric
from .validation import _validate_metric_label_name_token, _validate_metric_name

STREAM_LABEL = 'stream_id'

_EVICTED_EVENTS_KEPT = 1024


class DeltaCollectorError(ValueError):
    """Base class for errors raised by DeltaCollector."""


class InvalidDeltaError(DeltaCollectorError):
    """A batch or update is malformed (bad interval, negative delta, ...)."""


class BatchConflictError(DeltaCollectorError):
    """The sequence number was already committed with different content."""


class OutOfOrderBatchError(DeltaCollectorError):
    """The sequence number is stale or skips uncommitted batches."""


class StaleGenerationError(DeltaCollectorError):
    """A batch from an older generation arrived after a generation change."""


class IntervalOverlapError(DeltaCollectorError):
    """The batch interval overlaps the previously committed interval."""


class IntervalGapError(DeltaCollectorError):
    """The batch interval is not contiguous with the previous interval."""


class HistogramValidationError(InvalidDeltaError):
    """Histogram buckets/totals are inconsistent."""


class BucketLayoutChangeError(DeltaCollectorError):
    """The bucket layout changed without a new generation."""


@dataclass(frozen=True)
class CounterDelta:
    """A counter increment for one reporting interval."""
    name: str
    labels: Dict[str, str]
    value: float


@dataclass(frozen=True)
class HistogramDelta:
    """A histogram increment for one reporting interval.

    ``buckets`` are (upper bound, count) pairs with non-cumulative counts,
    sorted by upper bound. All bounds must be finite; the reported ``count``
    must equal the sum of the bucket counts.
    """
    name: str
    labels: Dict[str, str]
    count: int
    sum: float
    buckets: Tuple[Tuple[float, int], ...]


DeltaUpdate = Union[CounterDelta, HistogramDelta]


@dataclass(frozen=True)
class DeltaBatch:
    """One reported interval for a stream.

    ``sequence`` numbers batches within a generation, starting anywhere and
    increasing by exactly one per batch. ``interval_start`` of a batch must
    equal ``interval_end`` of the previous batch of the same generation.
    """
    stream_id: str
    generation: int
    sequence: int
    interval_start: float
    interval_end: float
    updates: Tuple[DeltaUpdate, ...]


@dataclass(frozen=True)
class EvictionEvent:
    """Record of a series evicted by the bounded label recycling policy."""
    stream_id: str
    generation: int
    name: str
    labels: Dict[str, str]
    sequence: int


@dataclass
class _CounterSeries:
    cumulative: float
    created: float


@dataclass
class _HistogramSeries:
    layout: Tuple[float, ...]
    buckets: List[int]  # cumulative, non-cumulative per bucket
    count: int
    sum: float
    created: float


@dataclass
class _StreamState:
    generation: int
    sequence: int
    interval_end: float
    fingerprint: Tuple
    series: 'OrderedDict[Tuple[str, Tuple[Tuple[str, str], ...]], Union[_CounterSeries, _HistogramSeries]]' = field(default_factory=OrderedDict)


def _labels_key(labels: Dict[str, str]) -> Tuple[Tuple[str, str], ...]:
    return tuple(sorted(labels.items()))


def _fingerprint(batch: DeltaBatch) -> Tuple:
    """Canonical form of a batch, used to detect identical replays."""
    updates = []
    for update in batch.updates:
        if isinstance(update, CounterDelta):
            updates.append(('counter', update.name, _labels_key(update.labels), float(update.value)))
        elif isinstance(update, HistogramDelta):
            updates.append((
                'histogram', update.name, _labels_key(update.labels),
                int(update.count), float(update.sum),
                tuple((float(bound), int(count)) for bound, count in update.buckets),
            ))
        else:
            raise InvalidDeltaError(f'unknown update type: {type(update)!r}')
    return (
        batch.stream_id, batch.generation, batch.sequence,
        float(batch.interval_start), float(batch.interval_end), tuple(updates),
    )


class DeltaCollector:
    """Reconstructs cumulative metrics from interval delta batches.

    Implements the collector protocol, so it can be registered directly
    with a CollectorRegistry.
    """

    def __init__(self, max_series_per_stream: int = 10000) -> None:
        if max_series_per_stream < 1:
            raise ValueError('max_series_per_stream must be at least 1')
        self._max_series_per_stream = max_series_per_stream
        self._lock = Lock()
        self._streams: Dict[str, _StreamState] = {}
        self._metric_kinds: Dict[str, str] = {}
        self._evictions: Deque[EvictionEvent] = deque(maxlen=_EVICTED_EVENTS_KEPT)
        self._eviction_counts: Dict[str, int] = {}

    @property
    def evictions(self) -> Sequence[EvictionEvent]:
        """The most recent eviction events (bounded)."""
        with self._lock:
            return tuple(self._evictions)

    def evictions_total(self, stream_id: Optional[str] = None) -> int:
        """Number of series evicted, overall or for one stream."""
        with self._lock:
            if stream_id is not None:
                return self._eviction_counts.get(stream_id, 0)
            return sum(self._eviction_counts.values())

    def apply(self, batch: DeltaBatch) -> bool:
        """Validate and commit a batch.

        Returns True if the batch was committed, False if it was an
        identical replay of the last committed batch. Raises a
        DeltaCollectorError subclass otherwise; a failed apply never
        modifies any state.
        """
        fingerprint = _fingerprint(batch)
        with self._lock:
            self._validate_batch_shape(batch)
            state = self._streams.get(batch.stream_id)
            if state is None or batch.generation > state.generation:
                # Bootstrap (also covers collector restarts) or an explicit
                # generation change: rebuild the cumulative state from
                # scratch, any sequence number is accepted as the start.
                self._validate_updates(batch, None)
                new_state = _StreamState(
                    generation=batch.generation,
                    sequence=batch.sequence,
                    interval_end=0.0,
                    fingerprint=(),
                )
                self._commit(batch, new_state, fingerprint)
                self._streams[batch.stream_id] = new_state
                return True
            if batch.generation < state.generation:
                raise StaleGenerationError(
                    f'stream {batch.stream_id!r} is at generation {state.generation}, '
                    f'rejecting late batch from generation {batch.generation}')
            if batch.sequence <= state.sequence:
                if batch.sequence == state.sequence and fingerprint == state.fingerprint:
                    return False
                if batch.sequence == state.sequence:
                    raise BatchConflictError(
                        f'stream {batch.stream_id!r} sequence {batch.sequence} '
                        'already committed with different content')
                raise OutOfOrderBatchError(
                    f'stream {batch.stream_id!r} sequence {batch.sequence} '
                    f'already committed (at {state.sequence})')
            if batch.sequence > state.sequence + 1:
                raise OutOfOrderBatchError(
                    f'stream {batch.stream_id!r} sequence gap: expected '
                    f'{state.sequence + 1}, got {batch.sequence}')
            if batch.interval_start < state.interval_end:
                raise IntervalOverlapError(
                    f'stream {batch.stream_id!r} interval '
                    f'[{batch.interval_start}, {batch.interval_end}) overlaps '
                    f'committed interval ending at {state.interval_end}')
            if batch.interval_start > state.interval_end:
                raise IntervalGapError(
                    f'stream {batch.stream_id!r} interval starts at '
                    f'{batch.interval_start} but previous interval ended at '
                    f'{state.interval_end}')
            self._validate_updates(batch, state)
            self._commit(batch, state, fingerprint)
            return True

    def _validate_batch_shape(self, batch: DeltaBatch) -> None:
        if not batch.stream_id:
            raise InvalidDeltaError('stream_id must not be empty')
        if batch.generation < 0:
            raise InvalidDeltaError('generation must not be negative')
        if batch.sequence < 0:
            raise InvalidDeltaError('sequence must not be negative')
        if not (math.isfinite(batch.interval_start) and math.isfinite(batch.interval_end)):
            raise InvalidDeltaError('interval bounds must be finite')
        if batch.interval_end <= batch.interval_start:
            raise InvalidDeltaError('interval_end must be greater than interval_start')

    def _validate_updates(self, batch: DeltaBatch, state: Optional[_StreamState]) -> None:
        pending_kinds: Dict[str, str] = {}
        pending_layouts: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Tuple[float, ...]] = {}
        for update in batch.updates:
            kind = 'counter' if isinstance(update, CounterDelta) else 'histogram'
            _validate_metric_name(update.name)
            existing_kind = pending_kinds.get(update.name, self._metric_kinds.get(update.name))
            if existing_kind is not None and existing_kind != kind:
                raise InvalidDeltaError(
                    f'metric {update.name!r} is already used as a {existing_kind}')
            pending_kinds[update.name] = kind
            for label_name in update.labels:
                _validate_metric_label_name_token(label_name)
                if label_name == STREAM_LABEL:
                    raise InvalidDeltaError(f'label {STREAM_LABEL!r} is reserved')
            if isinstance(update, CounterDelta):
                if not math.isfinite(update.value) or update.value < 0:
                    raise InvalidDeltaError(
                        f'counter delta for {update.name!r} must be a finite non-negative number')
            else:
                self._validate_histogram(update, state, pending_layouts)

    def _validate_histogram(self, update: HistogramDelta, state: Optional[_StreamState],
                            pending_layouts: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Tuple[float, ...]]) -> None:
        if not isinstance(update.count, int) or update.count < 0:
            raise HistogramValidationError(f'histogram count for {update.name!r} must be a non-negative int')
        if not math.isfinite(update.sum):
            raise HistogramValidationError(f'histogram sum for {update.name!r} must be finite')
        layout = []
        previous = None
        total = 0
        for bound, count in update.buckets:
            if not math.isfinite(bound):
                raise HistogramValidationError(
                    f'histogram bucket bounds for {update.name!r} must be finite')
            if previous is not None and bound <= previous:
                raise HistogramValidationError(
                    f'histogram bucket bounds for {update.name!r} must be strictly increasing')
            if not isinstance(count, int) or count < 0:
                raise HistogramValidationError(
                    f'histogram bucket counts for {update.name!r} must be non-negative ints')
            previous = bound
            layout.append(float(bound))
            total += count
        if total != update.count:
            raise HistogramValidationError(
                f'histogram bucket counts for {update.name!r} sum to {total}, '
                f'but count is {update.count}')
        key = (update.name, _labels_key(update.labels))
        existing_layout = pending_layouts.get(key)
        if existing_layout is None and state is not None:
            series = state.series.get(key)
            if series is not None:
                existing_layout = series.layout  # type: ignore[union-attr]
        layout_tuple = tuple(layout)
        if existing_layout is not None and existing_layout != layout_tuple:
            raise BucketLayoutChangeError(
                f'histogram {update.name!r} bucket layout changed from '
                f'{existing_layout} to {layout_tuple} without a new generation')
        pending_layouts[key] = layout_tuple

    def _commit(self, batch: DeltaBatch, state: _StreamState, fingerprint: Tuple) -> None:
        # Only called after the whole batch validated, so the state changes
        # below are atomic with respect to collect().
        for update in batch.updates:
            kind = 'counter' if isinstance(update, CounterDelta) else 'histogram'
            self._metric_kinds.setdefault(update.name, kind)
            key = (update.name, _labels_key(update.labels))
            series = state.series.get(key)
            if isinstance(update, CounterDelta):
                if series is None:
                    series = _CounterSeries(cumulative=0.0, created=batch.interval_start)
                    state.series[key] = series
                series.cumulative += update.value  # type: ignore[union-attr]
            else:
                if series is None:
                    series = _HistogramSeries(
                        layout=tuple(float(bound) for bound, _ in update.buckets),
                        buckets=[0] * len(update.buckets),
                        count=0,
                        sum=0.0,
                        created=batch.interval_start,
                    )
                    state.series[key] = series
                histogram = series  # type: ignore[assignment]
                for index, (_, count) in enumerate(update.buckets):
                    histogram.buckets[index] += count  # type: ignore[attr-defined]
                histogram.count += update.count  # type: ignore[attr-defined]
                histogram.sum += update.sum  # type: ignore[attr-defined]
            state.series.move_to_end(key)
        state.sequence = batch.sequence
        state.interval_end = batch.interval_end
        state.fingerprint = fingerprint
        while len(state.series) > self._max_series_per_stream:
            (name, labels_key), _ = state.series.popitem(last=False)
            event = EvictionEvent(
                stream_id=batch.stream_id,
                generation=batch.generation,
                name=name,
                labels=dict(labels_key),
                sequence=batch.sequence,
            )
            self._evictions.append(event)
            self._eviction_counts[batch.stream_id] = self._eviction_counts.get(batch.stream_id, 0) + 1

    def collect(self) -> Iterable[Metric]:
        families: Dict[str, Metric] = {}
        with self._lock:
            for stream_id, state in self._streams.items():
                for (name, labels_key), series in state.series.items():
                    labels = dict(labels_key)
                    labels[STREAM_LABEL] = stream_id
                    metric = families.get(name)
                    if metric is None:
                        metric = Metric(name, 'Reconstructed from delta reports.',
                                        self._metric_kinds.get(name, 'unknown'))
                        families[name] = metric
                    if isinstance(series, _CounterSeries):
                        metric.add_sample(name + '_total', labels, series.cumulative)
                        metric.add_sample(name + '_created', labels, series.created)
                    else:
                        cumulative = 0
                        for bound, count in zip(series.layout, series.buckets):
                            cumulative += count
                            bucket_labels = dict(labels)
                            bucket_labels['le'] = str(float(bound))
                            metric.add_sample(name + '_bucket', bucket_labels, float(cumulative))
                        inf_labels = dict(labels)
                        inf_labels['le'] = '+Inf'
                        metric.add_sample(name + '_bucket', inf_labels, float(series.count))
                        metric.add_sample(name + '_count', labels, float(series.count))
                        metric.add_sample(name + '_sum', labels, series.sum)
                        metric.add_sample(name + '_created', labels, series.created)
            metrics = list(families.values())
            if self._eviction_counts:
                evictions_metric = Metric(
                    'delta_collector_series_evictions',
                    'Series evicted by the bounded label recycling policy.',
                    'counter')
                for stream_id, count in sorted(self._eviction_counts.items()):
                    evictions_metric.add_sample(
                        'delta_collector_series_evictions_total',
                        {STREAM_LABEL: stream_id}, float(count))
                metrics.append(evictions_metric)
            return metrics
