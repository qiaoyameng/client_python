"""Exporter for the Prometheus protobuf (delimited MetricFamily) format.

Encodes metrics using the ``io.prometheus.client.MetricFamily`` schema from
`prometheus/client_model <https://github.com/prometheus/client_model>`_,
framed as a delimited stream: every message is prefixed with its length as
a varint. This is the format expected for the content type
``application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited``.

This module is a small dedicated encoder for that fixed schema, so no
protobuf runtime is required. Field numbers and wire types follow
``io/prometheus/client/metrics.proto`` exactly.

Coverage and degradation rules (mirroring the text exposition format):

* counter, gauge, summary, classic histogram and untyped (unknown) metrics
  are encoded natively, preserving labels, timestamps and exemplars.
* info metrics degrade to a gauge with the ``_info`` suffix, stateset
  metrics degrade to a gauge, and gaugehistogram metrics degrade to a
  classic histogram with their ``_gsum``/``_gcount`` series exposed as
  separate gauge families.
* native histogram samples cannot be represented in this schema and are
  rejected with a ``ValueError``.
"""

import struct
from typing import Dict, List, Optional, Tuple, Union

from .metrics_core import Metric
from .registry import Collector, REGISTRY
from .samples import Exemplar, Sample, Timestamp

CONTENT_TYPE_LATEST = 'application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited'
"""Content type of the delimited MetricFamily protobuf format."""

# MetricType enum values from io.prometheus.client.MetricType.
_METRIC_TYPE_COUNTER = 0
_METRIC_TYPE_GAUGE = 1
_METRIC_TYPE_SUMMARY = 2
_METRIC_TYPE_UNTYPED = 3
_METRIC_TYPE_HISTOGRAM = 4

# Protobuf wire types.
_WIRE_VARINT = 0
_WIRE_FIXED64 = 1
_WIRE_LENGTH_DELIMITED = 2


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def _tag(field_number: int, wire_type: int) -> bytes:
    return _varint((field_number << 3) | wire_type)


def _varint_field(field_number: int, value: int) -> bytes:
    # int64/uint64/enum. Negative int64 values are encoded as their 64-bit
    # two's complement, per the protobuf spec.
    return _tag(field_number, _WIRE_VARINT) + _varint(value & 0xFFFFFFFFFFFFFFFF)


def _double_field(field_number: int, value: float) -> bytes:
    # struct preserves negative zero, NaN and the infinities.
    return _tag(field_number, _WIRE_FIXED64) + struct.pack('<d', value)


def _bytes_field(field_number: int, value: bytes) -> bytes:
    return _tag(field_number, _WIRE_LENGTH_DELIMITED) + _varint(len(value)) + value


def _string_field(field_number: int, value: str) -> bytes:
    # Empty strings are encoded as a present field with zero length.
    return _bytes_field(field_number, value.encode('utf-8'))


def _to_millis(timestamp: Union[float, Timestamp]) -> int:
    return int(float(timestamp) * 1000)


def _label_pair(name: str, value: str) -> bytes:
    # LabelPair: name = 1, value = 2.
    return _string_field(1, name) + _string_field(2, value)


def _labels(labels: Dict[str, str]) -> bytes:
    # Metric.label = 1 (repeated LabelPair).
    return b''.join(_bytes_field(1, _label_pair(k, v)) for k, v in sorted(labels.items()))


def _exemplar(exemplar: Exemplar) -> bytes:
    # Exemplar: label = 1, value = 2, timestamp = 3 (milliseconds).
    out = b''.join(_bytes_field(1, _label_pair(k, v)) for k, v in sorted(exemplar.labels.items()))
    out += _double_field(2, float(exemplar.value))
    if exemplar.timestamp is not None:
        out += _varint_field(3, _to_millis(exemplar.timestamp))
    return out


def _group_key(labels: Dict[str, str], exclude: Tuple[str, ...] = ()) -> Tuple[Tuple[str, str], ...]:
    return tuple(sorted((k, v) for k, v in labels.items() if k not in exclude))


def _check_sample(sample: Sample) -> None:
    if sample.native_histogram is not None:
        raise ValueError(
            'Native histograms cannot be represented in the Prometheus protobuf format: ' + sample.name)


def _simple_metrics(metric: Metric, value_name: str, family_type: int) -> List[bytes]:
    created: Dict[Tuple[Tuple[str, str], ...], Sample] = {}
    values: List[Sample] = []
    for s in metric.samples:
        _check_sample(s)
        if s.name == value_name:
            values.append(s)
        elif s.name == metric.name + '_created':
            created[_group_key(s.labels)] = s
        else:
            raise ValueError(
                'Unsupported sample {!r} in {} metric {!r} for the Prometheus protobuf format'.format(
                    s.name, metric.type, metric.name))
    out = []
    for s in values:
        created_sample = created.get(_group_key(s.labels))
        created_ms = _to_millis(created_sample.value) if created_sample is not None else None
        out.append(_simple_metric(family_type, s, created_ms))
    return out


def _simple_metric(family_type: int, sample: Sample, created_ms: Optional[int]) -> bytes:
    msg = _labels(sample.labels)
    if family_type == _METRIC_TYPE_COUNTER:
        # Counter: value = 1, exemplar = 2, created_timestamp = 3.
        payload = _double_field(1, float(sample.value))
        if sample.exemplar is not None:
            payload += _bytes_field(2, _exemplar(sample.exemplar))
        if created_ms is not None:
            payload += _varint_field(3, created_ms)
        msg += _bytes_field(3, payload)  # Metric.counter = 3.
    elif family_type == _METRIC_TYPE_GAUGE:
        # Gauge: value = 1.
        msg += _bytes_field(2, _double_field(1, float(sample.value)))  # Metric.gauge = 2.
    else:
        # Untyped: value = 1.
        msg += _bytes_field(5, _double_field(1, float(sample.value)))  # Metric.untyped = 5.
    if sample.timestamp is not None:
        msg += _varint_field(6, _to_millis(sample.timestamp))  # Metric.timestamp_ms = 6.
    return msg


class _Group:
    def __init__(self, labels: Dict[str, str]) -> None:
        self.labels = labels
        self.timestamp: Optional[int] = None

    def observe_timestamp(self, sample: Sample) -> None:
        if self.timestamp is None and sample.timestamp is not None:
            self.timestamp = _to_millis(sample.timestamp)


class _SummaryGroup(_Group):
    def __init__(self, labels: Dict[str, str]) -> None:
        _Group.__init__(self, labels)
        self.count: Optional[Sample] = None
        self.sum: Optional[Sample] = None
        self.created: Optional[Sample] = None
        self.quantiles: List[Tuple[float, float]] = []

    def encode(self) -> bytes:
        # Summary: sample_count = 1, sample_sum = 2, quantile = 3,
        # created_timestamp = 4. Quantile: quantile = 1, value = 2.
        payload = b''
        if self.count is not None:
            payload += _varint_field(1, int(self.count.value))
        if self.sum is not None:
            payload += _double_field(2, float(self.sum.value))
        for quantile, value in self.quantiles:
            payload += _bytes_field(3, _double_field(1, quantile) + _double_field(2, value))
        if self.created is not None:
            payload += _varint_field(4, _to_millis(self.created.value))
        msg = _labels(self.labels) + _bytes_field(4, payload)  # Metric.summary = 4.
        if self.timestamp is not None:
            msg += _varint_field(6, self.timestamp)
        return msg


def _summary_metrics(metric: Metric) -> List[bytes]:
    groups: Dict[Tuple[Tuple[str, str], ...], _SummaryGroup] = {}
    ordered: List[Tuple[Tuple[str, str], ...]] = []

    def group_for(sample: Sample) -> _SummaryGroup:
        key = _group_key(sample.labels, ('quantile',))
        group = groups.get(key)
        if group is None:
            group = _SummaryGroup({k: v for k, v in sample.labels.items() if k != 'quantile'})
            groups[key] = group
            ordered.append(key)
        group.observe_timestamp(sample)
        return group

    for s in metric.samples:
        _check_sample(s)
        if s.name == metric.name:
            group_for(s).quantiles.append((float(s.labels['quantile']), float(s.value)))
        elif s.name == metric.name + '_count':
            group_for(s).count = s
        elif s.name == metric.name + '_sum':
            group_for(s).sum = s
        elif s.name == metric.name + '_created':
            group_for(s).created = s
        else:
            raise ValueError(
                'Unsupported sample {!r} in summary metric {!r} for the Prometheus protobuf format'.format(
                    s.name, metric.name))
    return [groups[key].encode() for key in ordered]


class _HistogramGroup(_Group):
    def __init__(self, labels: Dict[str, str]) -> None:
        _Group.__init__(self, labels)
        self.count: Optional[Sample] = None
        self.sum: Optional[Sample] = None
        self.created: Optional[Sample] = None
        self.buckets: List[Tuple[float, Sample]] = []

    def encode(self) -> bytes:
        # Histogram: sample_count = 1, sample_sum = 2, bucket = 3,
        # created_timestamp = 4.
        # Bucket: cumulative_count = 1, upper_bound = 2, exemplar = 3.
        payload = b''
        count = self.count
        if count is None and self.buckets:
            # The +Inf bucket, if present, holds the total count.
            upper, sample = max(self.buckets, key=lambda b: b[0])
            if upper == float('inf'):
                count = sample
        if count is not None:
            payload += _varint_field(1, int(count.value))
        if self.sum is not None:
            payload += _double_field(2, float(self.sum.value))
        for upper, sample in sorted(self.buckets, key=lambda b: b[0]):
            bucket = _varint_field(1, int(sample.value)) + _double_field(2, upper)
            if sample.exemplar is not None:
                bucket += _bytes_field(3, _exemplar(sample.exemplar))
            payload += _bytes_field(3, bucket)
        if self.created is not None:
            payload += _varint_field(4, _to_millis(self.created.value))
        msg = _labels(self.labels) + _bytes_field(7, payload)  # Metric.histogram = 7.
        if self.timestamp is not None:
            msg += _varint_field(6, self.timestamp)
        return msg


def _histogram_metrics(metric: Metric) -> List[bytes]:
    groups: Dict[Tuple[Tuple[str, str], ...], _HistogramGroup] = {}
    ordered: List[Tuple[Tuple[str, str], ...]] = []

    def group_for(sample: Sample) -> _HistogramGroup:
        key = _group_key(sample.labels, ('le',))
        group = groups.get(key)
        if group is None:
            group = _HistogramGroup({k: v for k, v in sample.labels.items() if k != 'le'})
            groups[key] = group
            ordered.append(key)
        group.observe_timestamp(sample)
        return group

    for s in metric.samples:
        _check_sample(s)
        if s.name == metric.name + '_bucket':
            group_for(s).buckets.append((float(s.labels['le']), s))
        elif s.name == metric.name + '_count':
            group_for(s).count = s
        elif s.name == metric.name + '_sum':
            group_for(s).sum = s
        elif s.name == metric.name + '_created':
            group_for(s).created = s
        else:
            raise ValueError(
                'Unsupported sample {!r} in histogram metric {!r} for the Prometheus protobuf format'.format(
                    s.name, metric.name))
    return [groups[key].encode() for key in ordered]


def _encode_family(name: str, documentation: str, family_type: int, metrics: List[bytes], unit: str) -> bytes:
    # MetricFamily: name = 1, help = 2, type = 3, metric = 4, unit = 5.
    out = _string_field(1, name)
    out += _string_field(2, documentation)
    out += _varint_field(3, family_type)
    for m in metrics:
        out += _bytes_field(4, m)
    if unit:
        out += _string_field(5, unit)
    return out


def _metric_families(metric: Metric) -> List[bytes]:
    mname = metric.name
    mtype = metric.type
    extra: List[bytes] = []
    if mtype == 'counter':
        mname += '_total'
        family_type = _METRIC_TYPE_COUNTER
        metrics = _simple_metrics(metric, mname, family_type)
    elif mtype == 'gauge':
        family_type = _METRIC_TYPE_GAUGE
        metrics = _simple_metrics(metric, mname, family_type)
    elif mtype == 'summary':
        family_type = _METRIC_TYPE_SUMMARY
        metrics = _summary_metrics(metric)
    elif mtype == 'histogram':
        family_type = _METRIC_TYPE_HISTOGRAM
        metrics = _histogram_metrics(metric)
    elif mtype == 'unknown':
        family_type = _METRIC_TYPE_UNTYPED
        metrics = _simple_metrics(metric, mname, family_type)
    elif mtype == 'info':
        # Info degrades to a gauge with the _info suffix, as in the text format.
        mname += '_info'
        family_type = _METRIC_TYPE_GAUGE
        metrics = _simple_metrics(metric, mname, family_type)
    elif mtype == 'stateset':
        # Stateset degrades to a gauge, as in the text format.
        family_type = _METRIC_TYPE_GAUGE
        metrics = _simple_metrics(metric, mname, family_type)
    elif mtype == 'gaugehistogram':
        # A gauge histogram degrades to a classic histogram, with its _gsum
        # and _gcount series exposed as separate gauge families, as in the
        # text format.
        family_type = _METRIC_TYPE_HISTOGRAM
        classic = Metric(metric.name, metric.documentation, 'histogram', metric.unit)
        classic.samples = [
            s for s in metric.samples
            if s.name in (metric.name + '_bucket', metric.name + '_created')
        ]
        metrics = _histogram_metrics(classic)
        for suffix in ('_gsum', '_gcount'):
            samples = [s for s in metric.samples if s.name == metric.name + suffix]
            if samples:
                extra.append(_encode_family(
                    metric.name + suffix, metric.documentation, _METRIC_TYPE_GAUGE,
                    [_simple_metric(_METRIC_TYPE_GAUGE, s, None) for s in samples], metric.unit))
    else:
        raise ValueError('Unsupported metric type for the Prometheus protobuf format: ' + mtype)
    return [_encode_family(mname, metric.documentation, family_type, metrics, metric.unit)] + extra


def generate_latest(registry: Collector = REGISTRY) -> bytes:
    """Returns the metrics from the registry in the delimited MetricFamily
    protobuf format as bytes."""
    output = []
    for metric in registry.collect():
        try:
            families = _metric_families(metric)
        except Exception as exception:
            exception.args = (exception.args or ('',)) + (metric,)
            raise
        for family in families:
            # Delimited framing: varint length prefix before each message.
            output.append(_varint(len(family)))
            output.append(family)
    return b''.join(output)
