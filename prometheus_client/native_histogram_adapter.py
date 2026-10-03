#!/usr/bin/env python

import math
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

from .metrics_core import Metric
from .registry import Collector
from .samples import BucketSpan, NativeHistogram, Sample
from .utils import floatToGoString

# Native histogram schemas -4 to 8 are the standard exponential schemas.
# Schema -53 marks custom bucket boundaries, which cannot be projected
# onto classic le boundaries by this adapter.
_MIN_SCHEMA = -4
_MAX_SCHEMA = 8
_CUSTOM_VALUES_SCHEMA = -53

# A native histogram bucket expanded to explicit boundaries as
# (lower, upper, count). Lower is exclusive and upper inclusive for
# positive buckets, and the other way around for negative buckets.
_Bucket = Tuple[float, float, float]


class NativeHistogramClassicAdapter(Collector):
    """Projects native histograms onto classic `le` bucket boundaries.

    Wraps a source collector (anything with a collect() method, such as
    another collector or a CollectorRegistry) and converts every native
    histogram sample it yields into classic histogram samples: one
    cumulative `<name>_bucket` sample per requested boundary plus a `+Inf`
    bucket, and `<name>_count`/`<name>_sum` samples. Labels, timestamps
    and any `<name>_created` sample are preserved, and the source samples
    are never modified. Families without native histogram samples, or
    without configured boundaries, pass through unchanged.

    `buckets` either maps a metric family name to the `le` boundaries to
    project onto, or is a single sequence of boundaries applied to every
    native histogram family. Boundaries must be finite, sorted and unique;
    the `+Inf` bucket is added automatically.

    In the default 'exact' mode a boundary that cuts through a non-empty
    native bucket raises ValueError, because the true cumulative count is
    unknowable. In 'estimate' mode such boundaries are instead reported as
    a clearly labelled interval estimate: a separate gauge family
    `<name>_bucket_estimate` carries, for each cut boundary, two samples
    with a `bound` label of 'lower' or 'upper' bracketing the true count.
    No averaged point estimate is ever presented as exact.
    """

    def __init__(self,
                 source: Collector,
                 buckets: Union[Sequence[float], Dict[str, Sequence[float]]],
                 mode: str = 'exact') -> None:
        if mode not in ('exact', 'estimate'):
            raise ValueError("mode must be 'exact' or 'estimate'")
        self._source = source
        self._mode = mode
        if isinstance(buckets, dict):
            self._bucket_map = {
                name: _validate_boundaries(les) for name, les in buckets.items()
            }
            self._default_boundaries: Optional[List[float]] = None
        else:
            self._bucket_map = {}
            self._default_boundaries = _validate_boundaries(buckets)

    def collect(self) -> Iterable[Metric]:
        for metric in self._source.collect():
            boundaries = self._default_boundaries
            if boundaries is None:
                boundaries = self._bucket_map.get(metric.name)
            if boundaries is None or not any(
                    s.native_histogram is not None for s in metric.samples):
                yield metric
                continue
            yield from self._convert(metric, boundaries)

    def _convert(self, metric: Metric, boundaries: Sequence[float]) -> Iterable[Metric]:
        classic = Metric(metric.name, metric.documentation, 'histogram', metric.unit)
        estimates = Metric(
            metric.name + '_bucket_estimate',
            ('Interval estimate of the cumulative count of {} observations up '
             'to le. le cuts across a native histogram bucket, so the exact '
             "count is unknown and lies between the samples labelled "
             "bound='lower' and bound='upper'.").format(metric.name),
            'gauge',
            metric.unit,
        )
        for sample in metric.samples:
            native_histogram = sample.native_histogram
            if native_histogram is None:
                # Pass through anything that is not a native histogram,
                # notably <name>_created samples.
                classic.samples.append(sample)
                continue
            self._convert_sample(
                metric.name, sample, native_histogram, boundaries, classic, estimates)
        yield classic
        if estimates.samples:
            yield estimates

    def _convert_sample(self,
                        name: str,
                        sample: Sample,
                        native_histogram: NativeHistogram,
                        boundaries: Sequence[float],
                        classic: Metric,
                        estimates: Metric) -> None:
        pos_buckets, neg_buckets = _expand_native_histogram(native_histogram, name)
        labels = dict(sample.labels) if sample.labels else {}
        for le in boundaries:
            low, high = _cumulative_interval(
                le, native_histogram.zero_threshold, native_histogram.zero_count,
                pos_buckets, neg_buckets)
            le_label = floatToGoString(le)
            if low == high:
                classic.samples.append(sample._replace(
                    name=name + '_bucket',
                    labels=dict(labels, le=le_label),
                    value=low,
                    exemplar=None,
                    native_histogram=None,
                ))
            elif self._mode == 'exact':
                raise ValueError(
                    'boundary le={} cuts through a non-empty native histogram '
                    'bucket of {}; the exact cumulative count is unknowable '
                    "(use mode='estimate' for an interval estimate)".format(le_label, name))
            else:
                for bound, value in (('lower', low), ('upper', high)):
                    estimates.samples.append(sample._replace(
                        name=name + '_bucket_estimate',
                        labels=dict(labels, le=le_label, bound=bound),
                        value=value,
                        exemplar=None,
                        native_histogram=None,
                    ))
        classic.samples.append(sample._replace(
            name=name + '_bucket',
            labels=dict(labels, le='+Inf'),
            value=native_histogram.count_value,
            exemplar=None,
            native_histogram=None,
        ))
        classic.samples.append(sample._replace(
            name=name + '_count',
            labels=dict(labels),
            value=native_histogram.count_value,
            exemplar=None,
            native_histogram=None,
        ))
        classic.samples.append(sample._replace(
            name=name + '_sum',
            labels=dict(labels),
            value=native_histogram.sum_value,
            exemplar=None,
            native_histogram=None,
        ))


def _validate_boundaries(boundaries: Sequence[float]) -> List[float]:
    les = [float(le) for le in boundaries]
    if not les:
        raise ValueError('at least one bucket boundary is required')
    if any(math.isinf(le) or math.isnan(le) for le in les):
        raise ValueError(
            'bucket boundaries must be finite; the +Inf bucket is added automatically')
    if les != sorted(les) or len(set(les)) != len(les):
        raise ValueError('bucket boundaries must be sorted in increasing order and unique')
    return les


def _expand_native_histogram(
        native_histogram: NativeHistogram, name: str) -> Tuple[List[_Bucket], List[_Bucket]]:
    """Validate a native histogram and expand its spans and delta-encoded
    counts into explicit (lower, upper, count) buckets."""
    if native_histogram.schema == _CUSTOM_VALUES_SCHEMA:
        raise ValueError(
            'native histogram {}: custom bucket boundaries (schema -53) '
            'are not supported'.format(name))
    if not _MIN_SCHEMA <= native_histogram.schema <= _MAX_SCHEMA:
        raise ValueError(
            'native histogram {}: invalid schema {}'.format(name, native_histogram.schema))
    if native_histogram.zero_threshold < 0:
        raise ValueError(
            'native histogram {}: negative zero threshold {}'.format(
                name, native_histogram.zero_threshold))
    if native_histogram.zero_count < 0:
        raise ValueError(
            'native histogram {}: negative zero count {}'.format(
                name, native_histogram.zero_count))
    if native_histogram.count_value < 0:
        raise ValueError(
            'native histogram {}: negative count {}'.format(name, native_histogram.count_value))
    base = 2.0 ** (2.0 ** -native_histogram.schema)
    pos_buckets = _expand_side(
        native_histogram.pos_spans, native_histogram.pos_deltas, base, True, name)
    neg_buckets = _expand_side(
        native_histogram.neg_spans, native_histogram.neg_deltas, base, False, name)
    total = native_histogram.zero_count
    total += sum(count for _, _, count in pos_buckets)
    total += sum(count for _, _, count in neg_buckets)
    if total != native_histogram.count_value:
        raise ValueError(
            'native histogram {}: bucket counts and zero count add up to {}, '
            'which does not match the count {}'.format(name, total, native_histogram.count_value))
    return pos_buckets, neg_buckets


def _expand_side(spans: Optional[Sequence[BucketSpan]],
                 deltas: Optional[Sequence[int]],
                 base: float,
                 positive: bool,
                 name: str) -> List[_Bucket]:
    spans = spans or []
    deltas = deltas or []
    buckets: List[_Bucket] = []
    count = 0.0
    index = 0
    bucket_number = 0
    for span_number, span in enumerate(spans):
        if span.length < 0:
            raise ValueError(
                'native histogram {}: span with negative length {}'.format(name, span.length))
        # The first span's offset is the index of its first bucket. Each
        # subsequent span's offset is the gap between the previous span's
        # last bucket and this span's first bucket.
        index = span.offset if span_number == 0 else index + span.offset
        for _ in range(span.length):
            if bucket_number >= len(deltas):
                raise ValueError(
                    'native histogram {}: spans cover more buckets than the '
                    '{} deltas given'.format(name, len(deltas)))
            count += deltas[bucket_number]
            bucket_number += 1
            if count < 0:
                raise ValueError('native histogram {}: negative bucket count'.format(name))
            upper = base ** index
            lower = base ** (index - 1)
            buckets.append((lower, upper, count) if positive else (-upper, -lower, count))
            index += 1
    if bucket_number != len(deltas):
        raise ValueError(
            'native histogram {}: spans cover {} buckets but {} deltas were '
            'given'.format(name, bucket_number, len(deltas)))
    return buckets


def _cumulative_interval(le: float,
                         zero_threshold: float,
                         zero_count: float,
                         pos_buckets: Sequence[_Bucket],
                         neg_buckets: Sequence[_Bucket]) -> Tuple[float, float]:
    """Bracket the cumulative count of observations up to and including le.

    Returns (low, high): low counts only buckets lying entirely at or below
    le, while high additionally counts every bucket that le cuts through.
    low == high means le aligns with the native bucket boundaries and the
    count is exact.
    """
    low = high = 0.0
    # The zero bucket covers [-zero_threshold, zero_threshold] inclusively.
    if le >= zero_threshold:
        low = high = zero_count
    elif le >= -zero_threshold:
        high += zero_count
    for lower, upper, count in pos_buckets:
        # Positive buckets are (lower, upper].
        if upper <= le:
            low += count
            high += count
        elif lower < le:
            high += count
    for lower, upper, count in neg_buckets:
        # Negative buckets are [lower, upper); a boundary equal to lower is
        # itself part of the bucket, so such a bucket is cut, not excluded.
        if upper <= le:
            low += count
            high += count
        elif lower <= le:
            high += count
    return low, high
