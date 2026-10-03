import math
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple, Union

from .metrics_core import Metric
from .registry import Collector
from .samples import BucketSpan, NativeHistogram, Sample, Timestamp
from .utils import floatToGoString


class ClassicHistogramProjection(NamedTuple):
    bounds: Tuple[float, ...]
    cumulative_counts: Tuple[int, ...]
    count: int
    sum_value: float
    created: Optional[Union[float, Timestamp]] = None


class ClassicBucketEstimate(NamedTuple):
    bound: float
    lower: int
    upper: int
    exact: bool


Bound = Union[float, int, str]


class _BucketInterval(NamedTuple):
    lower: float
    upper: float
    count: int
    upper_inclusive: bool
    zero_bucket: bool


def _as_bound(bound: Bound) -> float:
    if isinstance(bound, str):
        if bound == '+Inf':
            return float('inf')
        if bound == '-Inf':
            return float('-inf')
        try:
            value = float(bound)
        except ValueError:
            raise ValueError('Invalid histogram boundary: {!r}'.format(bound))
    else:
        value = float(bound)
    if math.isnan(value):
        raise ValueError('Histogram boundaries cannot be NaN')
    return value


def _normalise_bounds(bounds: Sequence[Bound], add_infinity: bool = True) -> Tuple[float, ...]:
    values = [_as_bound(bound) for bound in bounds]
    if any(left >= right for left, right in zip(values, values[1:])):
        raise ValueError('Histogram boundaries must be strictly increasing')
    if add_infinity and (not values or not math.isinf(values[-1]) or values[-1] < 0):
        values.append(float('inf'))
    return tuple(values)


def _non_negative_integer(value: float, description: str) -> int:
    if not isinstance(value, int) and not (isinstance(value, float) and math.isfinite(value) and value.is_integer()):
        raise ValueError('{} must be a non-negative integer'.format(description))
    result = int(value)
    if result < 0:
        raise ValueError('{} cannot be negative'.format(description))
    return result


def _decode_spans(spans: Optional[Sequence[BucketSpan]],
                  deltas: Optional[Sequence[int]],
                  side_name: str) -> Dict[int, int]:
    if spans is None and deltas is None:
        return {}
    if spans is None or deltas is None:
        raise ValueError(
            '{} spans and {} deltas must both be present or both be absent'.format(side_name, side_name))

    buckets: Dict[int, int] = {}
    expected_index: Optional[int] = None
    expected_deltas = 0
    for span_number, span in enumerate(spans):
        if not isinstance(span, BucketSpan):
            raise ValueError('{} span {} is invalid'.format(side_name, span_number))
        if span.length <= 0:
            raise ValueError('{} span {} has an invalid length'.format(side_name, span_number))
        if span_number == 0:
            index = span.offset
        else:
            if span.offset < 0:
                raise ValueError('{} span {} has an invalid offset'.format(side_name, span_number))
            index = expected_index + span.offset
        for _ in range(span.length):
            if index in buckets:
                raise ValueError('{} spans overlap at bucket index {}'.format(side_name, index))
            buckets[index] = 0
            index += 1
        expected_index = index
        expected_deltas += span.length

    if len(deltas) != expected_deltas:
        raise ValueError(
            'Expected {} {} deltas, got {}'.format(expected_deltas, side_name, len(deltas)))

    cumulative = 0
    for index, delta in zip(sorted(buckets), deltas):
        if not isinstance(delta, int):
            raise ValueError('{} deltas must be integers'.format(side_name))
        cumulative += delta
        if cumulative < 0:
            raise ValueError('{} bucket count at index {} is negative'.format(side_name, index))
        buckets[index] = cumulative
    return buckets


def decode_native_histogram(histogram: NativeHistogram) -> Tuple[Dict[int, int], Dict[int, int], int]:
    """Decode delta encoded native histogram buckets without modifying it."""
    if histogram.schema < -4 or histogram.schema > 8:
        raise ValueError('Unsupported native histogram schema: {}'.format(histogram.schema))
    if histogram.zero_threshold < 0 or math.isnan(histogram.zero_threshold) or math.isinf(histogram.zero_threshold):
        raise ValueError('Invalid native histogram zero threshold')

    zero_count = _non_negative_integer(histogram.zero_count, 'zero count')
    positive = _decode_spans(histogram.pos_spans, histogram.pos_deltas, 'positive')
    negative = _decode_spans(histogram.neg_spans, histogram.neg_deltas, 'negative')
    count = _non_negative_integer(histogram.count_value, 'count')

    total = zero_count + sum(positive.values()) + sum(negative.values())
    if total != count:
        raise ValueError('Native histogram count {} does not match bucket totals {}'.format(count, total))
    return positive, negative, zero_count


def _bucket_base(schema: int) -> float:
    return 2.0 ** (2.0 ** -schema)


def _bucket_bound(index: int, schema: int) -> float:
    return _bucket_base(schema) ** index


def _is_grid_boundary(value: float, schema: int) -> bool:
    if value == 0 or not math.isfinite(value):
        return False
    raw_index = math.log(abs(value)) / math.log(_bucket_base(schema))
    nearest = round(raw_index)
    return abs(raw_index - nearest) < 1e-10


def _is_exact_bound(bound: float, histogram: NativeHistogram) -> bool:
    if math.isinf(bound):
        return True

    zero_threshold = histogram.zero_threshold
    if bound == zero_threshold or bound == -zero_threshold:
        return True
    if -zero_threshold < bound < zero_threshold:
        return False
    return _is_grid_boundary(bound, histogram.schema)


def _bucket_intervals(histogram: NativeHistogram,
                      positive: Dict[int, int],
                      negative: Dict[int, int],
                      zero_count: int) -> List[_BucketInterval]:
    intervals: List[_BucketInterval] = []
    zero_threshold = histogram.zero_threshold
    intervals.append(_BucketInterval(
        -zero_threshold, zero_threshold, zero_count, True, True))
    for index, count in positive.items():
        lower = _bucket_bound(index, histogram.schema)
        upper = _bucket_bound(index + 1, histogram.schema)
        if upper <= zero_threshold:
            if count:
                raise ValueError('positive bucket {} overlaps the zero bucket'.format(index))
            continue
        intervals.append(_BucketInterval(
            max(lower, zero_threshold), upper, count, True, False))
    for index, count in negative.items():
        lower = -_bucket_bound(index + 1, histogram.schema)
        upper = -_bucket_bound(index, histogram.schema)
        if lower >= -zero_threshold:
            if count:
                raise ValueError('negative bucket {} overlaps the zero bucket'.format(index))
            continue
        clipped_upper = min(upper, -zero_threshold)
        intervals.append(_BucketInterval(
            lower,
            clipped_upper,
            count,
            clipped_upper != -zero_threshold,
            False,
        ))
    return intervals


def _interval_at_or_below(interval: _BucketInterval, bound: float) -> str:
    if bound < interval.lower:
        return 'none'
    if bound == interval.lower:
        return 'all' if interval.zero_bucket else 'none'
    if bound < interval.upper:
        return 'part'
    if bound == interval.upper:
        return 'all' if interval.upper_inclusive else 'none'
    return 'all'


def project_native_histogram_bounds(
        histogram: NativeHistogram,
        bounds: Sequence[Bound],
        created: Optional[Union[float, Timestamp]] = None,
        ) -> ClassicHistogramProjection:
    """Project a native histogram to exact cumulative classic histogram buckets."""
    estimates = estimate_native_histogram_bounds(histogram, bounds, add_infinity=True)
    cumulative: List[int] = []
    for estimate in estimates:
        if not estimate.exact:
            raise ValueError(
                'Boundary {} cuts through a native histogram bucket; exact projection is impossible'.format(
                    floatToGoString(estimate.bound)))
        cumulative.append(estimate.lower)

    return ClassicHistogramProjection(
        tuple(estimate.bound for estimate in estimates),
        tuple(cumulative),
        _non_negative_integer(histogram.count_value, 'count'),
        histogram.sum_value,
        created,
    )


def estimate_native_histogram_bounds(
        histogram: NativeHistogram,
        bounds: Sequence[Bound],
        add_infinity: bool = False,
        ) -> Tuple[ClassicBucketEstimate, ...]:
    """Return explicit lower/upper cumulative count estimates for each boundary."""
    positive, negative, zero_count = decode_native_histogram(histogram)
    normalised_bounds = _normalise_bounds(bounds, add_infinity=add_infinity)
    intervals = _bucket_intervals(histogram, positive, negative, zero_count)
    estimates: List[ClassicBucketEstimate] = []

    for bound in normalised_bounds:
        lower = 0
        upper = 0
        exact = _is_exact_bound(bound, histogram)
        if math.isinf(bound):
            lower = upper = 0 if bound < 0 else int(histogram.count_value)
        else:
            for interval in intervals:
                relation = _interval_at_or_below(interval, bound)
                if relation == 'all':
                    lower += interval.count
                    upper += interval.count
                elif relation == 'part':
                    upper += interval.count
                    exact = False
        estimates.append(ClassicBucketEstimate(float(bound), lower, upper, exact))
    return tuple(estimates)


class NativeHistogramClassicCollector:
    """Project native histograms from a collector into classic histograms."""

    def __init__(self,
                 collector: Collector,
                 bounds: Union[Sequence[Bound], Dict[str, Sequence[Bound]]],
                 ) -> None:
        self._collector = collector
        if isinstance(bounds, dict):
            self._bounds = {name: _normalise_bounds(metric_bounds) for name, metric_bounds in bounds.items()}
        else:
            self._bounds = _normalise_bounds(bounds)

    def describe(self) -> Iterable[Metric]:
        describe = getattr(self._collector, 'describe', None)
        if describe is not None:
            return iter(describe())
        else:
            yield from self.collect()

    def collect(self) -> Iterable[Metric]:
        for source_metric in self._collector.collect():
            if source_metric.type != 'histogram':
                yield source_metric
                continue
            yield self._convert_metric(source_metric)

    def _bounds_for(self, metric_name: str) -> Tuple[float, ...]:
        if isinstance(self._bounds, dict):
            if metric_name not in self._bounds:
                raise ValueError('No classic histogram boundaries configured for {}'.format(metric_name))
            return self._bounds[metric_name]
        return self._bounds

    def _convert_metric(self, source_metric: Metric) -> Metric:
        metric = Metric(source_metric.name, source_metric.documentation, source_metric.type, source_metric.unit)
        groups: Dict[Tuple[Tuple[str, str], ...], Dict[str, Sample]] = {}
        native_labels = set()
        native_count = 0

        for sample in source_metric.samples:
            if sample.native_histogram is not None:
                native_count += 1
                labels = tuple(sorted(sample.labels.items())) if sample.labels else ()
                if labels in groups:
                    raise ValueError('Duplicate native histogram for {} {}'.format(source_metric.name, labels))
                native_labels.add(labels)
                groups.setdefault(labels, {})['native'] = sample
            elif sample.name == source_metric.name + '_created':
                labels = tuple(sorted(sample.labels.items())) if sample.labels else ()
                groups.setdefault(labels, {})['created'] = sample
            else:
                if sample.name == source_metric.name + '_bucket':
                    labels = tuple(sorted(
                        (name, value) for name, value in sample.labels.items() if name != 'le'))
                else:
                    labels = tuple(sorted(sample.labels.items())) if sample.labels else ()
                groups.setdefault(labels, {}).setdefault('samples', []).append(sample)

        if native_count == 0:
            return source_metric

        for labels, group in groups.items():
            if labels not in native_labels:
                for sample in group.get('samples', ()):
                    metric.samples.append(sample)
                created_sample = group.get('created')
                if created_sample is not None:
                    metric.samples.append(created_sample)
                continue
            native_sample = group['native']
            label_dict = dict(labels)
            histogram = native_sample.native_histogram
            created_sample = group.get('created')
            created = created_sample.value if created_sample is not None else None
            projection = project_native_histogram_bounds(
                histogram,
                self._bounds_for(source_metric.name),
                created,
            )

            for bound, value in zip(projection.bounds, projection.cumulative_counts):
                bucket_labels = dict(label_dict)
                bucket_labels['le'] = floatToGoString(bound)
                metric.samples.append(Sample(
                    source_metric.name + '_bucket',
                    bucket_labels,
                    value,
                    native_sample.timestamp,
                ))

            count_name = '_gcount' if source_metric.type == 'gaugehistogram' else '_count'
            sum_name = '_gsum' if source_metric.type == 'gaugehistogram' else '_sum'
            metric.samples.append(Sample(
                source_metric.name + count_name,
                dict(label_dict),
                projection.count,
                native_sample.timestamp,
            ))
            metric.samples.append(Sample(
                source_metric.name + sum_name,
                dict(label_dict),
                projection.sum_value,
                native_sample.timestamp,
            ))
            if created_sample is not None:
                metric.samples.append(Sample(
                    source_metric.name + '_created',
                    dict(label_dict),
                    created_sample.value,
                    created_sample.timestamp,
                ))
        return metric
