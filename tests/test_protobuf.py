import math
import struct
import unittest

from prometheus_client import protobuf
from prometheus_client.exposition import (
    choose_encoder, CONTENT_TYPE_PLAIN_0_0_4, generate_latest,
)
from prometheus_client.metrics_core import (
    CounterMetricFamily, GaugeHistogramMetricFamily, GaugeMetricFamily,
    HistogramMetricFamily, InfoMetricFamily, Metric, StateSetMetricFamily,
    UnknownMetricFamily,
)
from prometheus_client.registry import CollectorRegistry
from prometheus_client.samples import Exemplar, NativeHistogram


# Minimal independent reference decoder for the io.prometheus.client
# schema, used to verify the encoder's framing and nested fields.
def read_varint(buf, pos):
    result = 0
    shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def decode_message(buf):
    """Decode a protobuf message into {field_number: [values]}.

    Varints are returned as ints, 64-bit values as floats and
    length-delimited values as raw bytes.
    """
    fields = {}
    pos = 0
    while pos < len(buf):
        tag, pos = read_varint(buf, pos)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value, pos = read_varint(buf, pos)
        elif wire == 1:
            value = struct.unpack('<d', buf[pos:pos + 8])[0]
            pos += 8
        elif wire == 2:
            length, pos = read_varint(buf, pos)
            value = buf[pos:pos + length]
            pos += length
        else:
            raise AssertionError('unexpected wire type: %d' % wire)
        fields.setdefault(field, []).append(value)
    return fields


def decode_stream(data):
    """Decode a delimited stream of MetricFamily messages."""
    families = []
    pos = 0
    while pos < len(data):
        length, pos = read_varint(data, pos)
        families.append(decode_message(data[pos:pos + length]))
        pos += length
    return families


def encode(*metrics):
    registry = CollectorRegistry()

    class CustomCollector:
        def collect(self):
            return list(metrics)

    registry.register(CustomCollector())
    return protobuf.generate_latest(registry)


def labels_of(metric_msg):
    return {
        decode_message(lp)[1][0].decode(): decode_message(lp)[2][0].decode()
        for lp in metric_msg.get(1, [])
    }


class TestProtobufEncoding(unittest.TestCase):
    def test_gauge_fixture_exact_bytes(self):
        # Fixed binary fixture: one gauge family with a label, value 1.5
        # and timestamp 1000ms, hand-assembled per metrics.proto.
        gauge = GaugeMetricFamily('my_gauge', 'A gauge.', labels=['l'])
        gauge.add_metric(['v'], 1.5, timestamp=1.0)

        label_pair = b'\x0a\x01l\x12\x01v'
        gauge_msg = b'\x09\x00\x00\x00\x00\x00\x00\xf8\x3f'
        metric_msg = (
            b'\x0a' + bytes([len(label_pair)]) + label_pair
            + b'\x12' + bytes([len(gauge_msg)]) + gauge_msg
            + b'\x30\xe8\x07'
        )
        family = (
            b'\x0a\x08my_gauge'
            + b'\x12\x08A gauge.'
            + b'\x18\x01'
            + b'\x22' + bytes([len(metric_msg)]) + metric_msg
        )
        expected = bytes([len(family)]) + family
        self.assertEqual(expected, encode(gauge))

    def test_counter(self):
        counter = CounterMetricFamily('my_counter', 'A counter.', labels=['l'])
        counter.add_metric(
            ['v'], 5.0, created=1600000000.0, timestamp=1.5,
            exemplar=Exemplar({'trace_id': 'abc'}, 2.0, 1.5))

        family, = decode_stream(encode(counter))
        self.assertEqual(b'my_counter_total', family[1][0])
        self.assertEqual(b'A counter.', family[2][0])
        self.assertEqual(0, family[3][0])  # COUNTER

        metric, = family[4]
        metric = decode_message(metric)
        self.assertEqual({'l': 'v'}, labels_of(metric))
        self.assertEqual(1500, metric[6][0])  # timestamp_ms

        counter_msg = decode_message(metric[3][0])
        self.assertEqual(5.0, counter_msg[1][0])
        self.assertEqual(1600000000000, counter_msg[3][0])  # created_timestamp

        exemplar = decode_message(counter_msg[2][0])
        self.assertEqual({'trace_id': 'abc'}, labels_of(exemplar))
        self.assertEqual(2.0, exemplar[2][0])
        self.assertEqual(1500, exemplar[3][0])

    def test_summary(self):
        summary = Metric('my_summary', 'A summary.', 'summary')
        summary.add_sample('my_summary', {'quantile': '0.5'}, 10.0)
        summary.add_sample('my_summary', {'quantile': '0.9'}, 20.0)
        summary.add_sample('my_summary_count', {}, 2, timestamp=1.0)
        summary.add_sample('my_summary_sum', {}, 30.0)
        summary.add_sample('my_summary_created', {}, 1600000000.0)

        family, = decode_stream(encode(summary))
        self.assertEqual(b'my_summary', family[1][0])
        self.assertEqual(2, family[3][0])  # SUMMARY

        metric, = family[4]
        metric = decode_message(metric)
        self.assertEqual({}, labels_of(metric))
        self.assertEqual(1000, metric[6][0])

        summary_msg = decode_message(metric[4][0])
        self.assertEqual(2, summary_msg[1][0])
        self.assertEqual(30.0, summary_msg[2][0])
        self.assertEqual(1600000000000, summary_msg[4][0])
        quantiles = [decode_message(q) for q in summary_msg[3]]
        self.assertEqual([(0.5, 10.0), (0.9, 20.0)], [(q[1][0], q[2][0]) for q in quantiles])

    def test_histogram(self):
        histogram = HistogramMetricFamily(
            'my_histogram', 'A histogram.',
            buckets=[('0.1', 1.0), ('+Inf', 2.0, Exemplar({'trace_id': 'x'}, 3.0, 2.0))],
            sum_value=1.5)

        family, = decode_stream(encode(histogram))
        self.assertEqual(b'my_histogram', family[1][0])
        self.assertEqual(4, family[3][0])  # HISTOGRAM

        metric, = family[4]
        metric = decode_message(metric)
        histogram_msg = decode_message(metric[7][0])
        self.assertEqual(2, histogram_msg[1][0])  # sample_count
        self.assertEqual(1.5, histogram_msg[2][0])  # sample_sum

        buckets = [decode_message(b) for b in histogram_msg[3]]
        self.assertEqual([(1, 0.1), (2, math.inf)], [(b[1][0], b[2][0]) for b in buckets])
        exemplar = decode_message(buckets[1][3][0])
        self.assertEqual({'trace_id': 'x'}, labels_of(exemplar))
        self.assertEqual(3.0, exemplar[2][0])
        self.assertEqual(2000, exemplar[3][0])

    def test_untyped(self):
        unknown = UnknownMetricFamily('my_unknown', 'An unknown.', value=3.0)

        family, = decode_stream(encode(unknown))
        self.assertEqual(b'my_unknown', family[1][0])
        self.assertEqual(3, family[3][0])  # UNTYPED

        metric, = family[4]
        metric = decode_message(metric)
        untyped = decode_message(metric[5][0])
        self.assertEqual(3.0, untyped[1][0])

    def test_special_doubles(self):
        gauge = GaugeMetricFamily('special', 'Special values.', labels=['case'])
        gauge.add_metric(['negzero'], -0.0)
        gauge.add_metric(['nan'], float('nan'))
        gauge.add_metric(['posinf'], float('inf'))
        gauge.add_metric(['neginf'], float('-inf'))

        family, = decode_stream(encode(gauge))
        values = {}
        for raw in family[4]:
            metric = decode_message(raw)
            values[labels_of(metric)['case']] = decode_message(metric[2][0])[1][0]
        self.assertEqual(0.0, values['negzero'])
        self.assertEqual(-1.0, math.copysign(1.0, values['negzero']))
        self.assertTrue(math.isnan(values['nan']))
        self.assertEqual(math.inf, values['posinf'])
        self.assertEqual(-math.inf, values['neginf'])

    def test_empty_strings(self):
        gauge = GaugeMetricFamily('empty', '', labels=['l'])
        gauge.add_metric([''], 1.0)

        family, = decode_stream(encode(gauge))
        self.assertEqual(b'', family[2][0])  # empty help is still present
        metric, = family[4]
        metric = decode_message(metric)
        self.assertEqual({'l': ''}, labels_of(metric))

    def test_native_histogram_rejected(self):
        metric = Metric('my_histogram', 'A histogram.', 'histogram')
        metric.add_sample(
            'my_histogram', {}, 0.0,
            native_histogram=NativeHistogram(
                count_value=1.0, sum_value=2.0, schema=0,
                zero_threshold=0.001, zero_count=0.0))
        with self.assertRaises(ValueError):
            encode(metric)

    def test_info_degrades_to_gauge(self):
        info = InfoMetricFamily('my_build', 'Build info.', value={'version': '1.0'})

        family, = decode_stream(encode(info))
        self.assertEqual(b'my_build_info', family[1][0])
        self.assertEqual(1, family[3][0])  # GAUGE
        metric, = family[4]
        metric = decode_message(metric)
        self.assertEqual({'version': '1.0'}, labels_of(metric))
        self.assertEqual(1.0, decode_message(metric[2][0])[1][0])

    def test_stateset_degrades_to_gauge(self):
        stateset = StateSetMetricFamily('my_state', 'State.', value={'a': True, 'b': False})

        family, = decode_stream(encode(stateset))
        self.assertEqual(b'my_state', family[1][0])
        self.assertEqual(1, family[3][0])  # GAUGE
        values = {}
        for raw in family[4]:
            metric = decode_message(raw)
            values[labels_of(metric)['my_state']] = decode_message(metric[2][0])[1][0]
        self.assertEqual({'a': 1.0, 'b': 0.0}, values)

    def test_gaugehistogram_degrades(self):
        gaugehistogram = GaugeHistogramMetricFamily(
            'my_ghistogram', 'A gauge histogram.',
            buckets=[('0.1', 1.0), ('+Inf', 2.0)], gsum_value=3.0)

        families = decode_stream(encode(gaugehistogram))
        self.assertEqual(
            [b'my_ghistogram', b'my_ghistogram_gsum', b'my_ghistogram_gcount'],
            [f[1][0] for f in families])
        self.assertEqual(4, families[0][3][0])  # HISTOGRAM
        self.assertEqual(1, families[1][3][0])  # GAUGE
        self.assertEqual(1, families[2][3][0])  # GAUGE

        metric, = families[0][4]
        histogram_msg = decode_message(decode_message(metric)[7][0])
        buckets = [decode_message(b) for b in histogram_msg[3]]
        self.assertEqual([(1, 0.1), (2, math.inf)], [(b[1][0], b[2][0]) for b in buckets])

        gsum = decode_message(decode_message(families[1][4][0])[2][0])
        self.assertEqual(3.0, gsum[1][0])
        gcount = decode_message(decode_message(families[2][4][0])[2][0])
        self.assertEqual(2.0, gcount[1][0])

    def test_unit(self):
        gauge = GaugeMetricFamily('my_ratio', 'A ratio.', value=0.5, unit='ratio')
        family, = decode_stream(encode(gauge))
        self.assertEqual(b'ratio', family[5][0])


class TestChooseEncoder(unittest.TestCase):
    def test_protobuf_selected(self):
        generator, content_type = choose_encoder(
            'application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited')
        self.assertEqual(protobuf.generate_latest, generator)
        self.assertEqual(protobuf.CONTENT_TYPE_LATEST, content_type)

    def test_protobuf_selected_with_qvalue(self):
        generator, content_type = choose_encoder('application/vnd.google.protobuf; q=0.5')
        self.assertEqual(protobuf.generate_latest, generator)
        self.assertEqual(protobuf.CONTENT_TYPE_LATEST, content_type)

    def test_protobuf_q_zero_not_selected(self):
        generator, content_type = choose_encoder('application/vnd.google.protobuf; q=0')
        self.assertEqual(generate_latest, generator)
        self.assertEqual(CONTENT_TYPE_PLAIN_0_0_4, content_type)

    def test_protobuf_q_zero_falls_through_to_text(self):
        generator, content_type = choose_encoder(
            'application/vnd.google.protobuf; q=0, text/plain; version=1.0.0')
        self.assertEqual(CONTENT_TYPE_PLAIN_0_0_4.replace('0.0.4', '1.0.0') + '; escaping=underscores', content_type)

    def test_default_unchanged(self):
        generator, content_type = choose_encoder(None)
        self.assertEqual(generate_latest, generator)
        self.assertEqual(CONTENT_TYPE_PLAIN_0_0_4, content_type)


if __name__ == '__main__':
    unittest.main()
