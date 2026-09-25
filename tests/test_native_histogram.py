import math
import threading
import unittest

from prometheus_client import CollectorRegistry, NativeHistogramMetric
from prometheus_client.core import BucketSpan
from prometheus_client.openmetrics.exposition import generate_latest
from prometheus_client.openmetrics.parser import text_string_to_metric_families


def decode_buckets(spans, deltas):
    """Decode BucketSpan/delta encoding into an {index: count} dict."""
    buckets = {}
    if not spans:
        return buckets
    index = spans[0].offset
    delta_index = 0
    count = 0
    for i, span in enumerate(spans):
        if i > 0:
            index += span.offset
        for _ in range(span.length):
            count += deltas[delta_index]
            buckets[index] = count
            index += 1
            delta_index += 1
    return buckets


def get_native_histogram(metric):
    samples = list(metric.collect())[0].samples
    nh_samples = [s for s in samples if s.native_histogram is not None]
    assert len(nh_samples) == 1
    return nh_samples[0].native_histogram


class TestNativeHistogramMetric(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()

    def test_bucket_boundaries_schema_0(self):
        # Schema 0: growth factor 2, bucket i covers (2^(i-1), 2^i].
        h = NativeHistogramMetric('nh', 'doc', schema=0, registry=self.registry)
        for v in [1, 2, 4, 8, 1.5]:
            h.observe(v)
        nh = get_native_histogram(h)
        self.assertEqual(5, nh.count_value)
        self.assertEqual(16.5, nh.sum_value)
        self.assertEqual(0, nh.zero_count)
        self.assertEqual({0: 1, 1: 2, 2: 1, 3: 1}, decode_buckets(nh.pos_spans, nh.pos_deltas))
        self.assertEqual({}, decode_buckets(nh.neg_spans, nh.neg_deltas))
        self.assertEqual((BucketSpan(0, 4),), tuple(nh.pos_spans))
        self.assertEqual((1, 1, -1, 0), tuple(nh.pos_deltas))

    def test_exact_boundaries_higher_schema(self):
        # Schema n: 2^m falls exactly on the boundary of bucket m * 2^n.
        h = NativeHistogramMetric('nh', 'doc', schema=3, registry=self.registry)
        for m in [-2, -1, 0, 1, 2]:
            h.observe(2.0 ** m)
        nh = get_native_histogram(h)
        expected = {8 * m: 1 for m in [-2, -1, 0, 1, 2]}
        self.assertEqual(expected, decode_buckets(nh.pos_spans, nh.pos_deltas))

    def test_positive_negative_mirror(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, registry=self.registry)
        h.observe(2)
        h.observe(-2)
        h.observe(-3)
        nh = get_native_histogram(h)
        self.assertEqual(3, nh.count_value)
        self.assertEqual(-3.0, nh.sum_value)
        self.assertEqual({1: 1}, decode_buckets(nh.pos_spans, nh.pos_deltas))
        # -2 -> bucket 1, -3 -> bucket 2, mirroring the positive side.
        self.assertEqual({1: 1, 2: 1}, decode_buckets(nh.neg_spans, nh.neg_deltas))

    def test_zero_bucket(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, zero_threshold=0.5, registry=self.registry)
        for v in [0, 0.5, -0.5, 0.25, -0.25]:
            h.observe(v)
        h.observe(0.6)
        nh = get_native_histogram(h)
        self.assertEqual(6, nh.count_value)
        self.assertEqual(5, nh.zero_count)
        self.assertEqual(0.5, nh.zero_threshold)
        # 0.6 is in (0.5, 1] -> bucket 0.
        self.assertEqual({0: 1}, decode_buckets(nh.pos_spans, nh.pos_deltas))

    def test_zero_threshold_zero(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, zero_threshold=0, registry=self.registry)
        h.observe(0)
        nh = get_native_histogram(h)
        self.assertEqual(1, nh.zero_count)
        self.assertIsNone(nh.pos_spans)
        self.assertIsNone(nh.neg_spans)

    def test_empty(self):
        h = NativeHistogramMetric('nh', 'doc', registry=self.registry)
        nh = get_native_histogram(h)
        self.assertEqual(0, nh.count_value)
        self.assertEqual(0.0, nh.sum_value)
        self.assertEqual(0, nh.zero_count)
        self.assertIsNone(nh.pos_spans)
        self.assertIsNone(nh.neg_spans)

    def test_resolution_reduction(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, max_buckets=2, registry=self.registry)
        for v in [1, 2, 4]:
            h.observe(v)
        nh = get_native_histogram(h)
        # 3 buckets over the limit of 2: schema reduced 0 -> -1 (factor 4),
        # merging buckets 1 and 2.
        self.assertEqual(-1, nh.schema)
        self.assertEqual(3, nh.count_value)
        self.assertEqual({0: 1, 1: 2}, decode_buckets(nh.pos_spans, nh.pos_deltas))

        # New observations use the reduced schema: 16 = 4^2 -> bucket 2.
        h.observe(16)
        nh = get_native_histogram(h)
        # 3 buckets again: reduced once more to schema -2 (factor 16).
        self.assertEqual(-2, nh.schema)
        self.assertEqual(4, nh.count_value)
        self.assertEqual(23.0, nh.sum_value)
        self.assertEqual({0: 1, 1: 3}, decode_buckets(nh.pos_spans, nh.pos_deltas))

    def test_multiple_resolution_reductions_single_observe(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, max_buckets=1, registry=self.registry)
        for v in [2, 4, 16]:
            h.observe(v)
        nh = get_native_histogram(h)
        # 3 buckets, limit 1: reduced twice, down to schema -2 (factor 16).
        self.assertEqual(-2, nh.schema)
        self.assertEqual(3, nh.count_value)
        self.assertEqual({1: 3}, decode_buckets(nh.pos_spans, nh.pos_deltas))

    def test_reduction_floor_preserves_observations(self):
        # 1 and 2 never share a bucket at any schema, so the limit of 1
        # cannot be met: the schema bottoms out at -4 and no observation
        # is dropped.
        h = NativeHistogramMetric('nh', 'doc', schema=0, max_buckets=1, registry=self.registry)
        h.observe(1)
        h.observe(2)
        nh = get_native_histogram(h)
        self.assertEqual(-4, nh.schema)
        self.assertEqual(2, nh.count_value)
        self.assertEqual({0: 1, 1: 1}, decode_buckets(nh.pos_spans, nh.pos_deltas))

    def test_reduction_preserves_all_observations(self):
        values = [2.0 ** i for i in range(-10, 11)]
        h = NativeHistogramMetric('nh', 'doc', schema=2, max_buckets=4, registry=self.registry)
        for v in values:
            h.observe(v)
        nh = get_native_histogram(h)
        self.assertEqual(-3, nh.schema)
        self.assertEqual(len(values), nh.count_value)
        self.assertEqual(sum(values), nh.sum_value)
        # At schema -3 (factor 256), 2^i lands in bucket ceil(i / 8).
        expected = {}
        for i in range(-10, 11):
            index = math.ceil(i / 8)
            expected[index] = expected.get(index, 0) + 1
        self.assertEqual(expected, decode_buckets(nh.pos_spans, nh.pos_deltas))
        total = sum(decode_buckets(nh.pos_spans, nh.pos_deltas).values())
        self.assertEqual(len(values), total)

    def test_reduction_with_both_signs(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, max_buckets=2, registry=self.registry)
        for v in [2, 4, -2, -4]:
            h.observe(v)
        nh = get_native_histogram(h)
        # 4 buckets over the limit of 2: reduced to schema -1.
        self.assertEqual(-1, nh.schema)
        self.assertEqual(4, nh.count_value)
        self.assertEqual({1: 2}, decode_buckets(nh.pos_spans, nh.pos_deltas))
        self.assertEqual({1: 2}, decode_buckets(nh.neg_spans, nh.neg_deltas))

    def test_nan_rejected(self):
        h = NativeHistogramMetric('nh', 'doc', registry=self.registry)
        with self.assertRaises(ValueError):
            h.observe(float('nan'))
        nh = get_native_histogram(h)
        self.assertEqual(0, nh.count_value)
        self.assertEqual(0.0, nh.sum_value)

    def test_inf_observations(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, registry=self.registry)
        h.observe(float('inf'))
        h.observe(float('-inf'))
        nh = get_native_histogram(h)
        self.assertEqual(2, nh.count_value)
        self.assertTrue(math.isnan(nh.sum_value))
        # +Inf is clamped into the outermost bucket (index 1024 at schema 0).
        self.assertEqual({1024: 1}, decode_buckets(nh.pos_spans, nh.pos_deltas))
        self.assertEqual({1024: 1}, decode_buckets(nh.neg_spans, nh.neg_deltas))

    def test_extreme_floats(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, zero_threshold=0, registry=self.registry)
        h.observe(1.7976931348623157e308)  # sys.float_info.max
        h.observe(5e-324)  # smallest positive denormal
        nh = get_native_histogram(h)
        self.assertEqual(2, nh.count_value)
        # Largest finite double sits in the outermost bucket, the smallest
        # denormal exactly on the boundary of bucket -1074.
        self.assertEqual({1024: 1, -1074: 1}, decode_buckets(nh.pos_spans, nh.pos_deltas))

    def test_invalid_config(self):
        for kwargs in ({'schema': 9}, {'schema': -5}, {'schema': 1.5},
                       {'zero_threshold': -1}, {'zero_threshold': float('nan')},
                       {'zero_threshold': float('inf')},
                       {'max_buckets': 0}, {'max_buckets': -1}):
            with self.assertRaises(ValueError, msg=kwargs):
                NativeHistogramMetric('nh_bad', 'doc', registry=self.registry, **kwargs)

    def test_labels(self):
        h = NativeHistogramMetric('nh', 'doc', labelnames=['l'], schema=0, registry=self.registry)
        h.labels('a').observe(2)
        h.labels('b').observe(4)
        h.labels('b').observe(-4)
        metrics = list(h.collect())[0]
        by_label = {s.labels['l']: s.native_histogram for s in metrics.samples if s.native_histogram}
        self.assertEqual({1: 1}, decode_buckets(by_label['a'].pos_spans, by_label['a'].pos_deltas))
        self.assertEqual({2: 1}, decode_buckets(by_label['b'].pos_spans, by_label['b'].pos_deltas))
        self.assertEqual({2: 1}, decode_buckets(by_label['b'].neg_spans, by_label['b'].neg_deltas))

    def test_exposition_and_parse_roundtrip(self):
        h = NativeHistogramMetric('nh_roundtrip', 'A native histogram', schema=0, registry=self.registry)
        for v in [0.1, 1, 2, 3, -4, -100, 0]:
            h.observe(v)
        expected = get_native_histogram(h)

        output = generate_latest(self.registry, version="2.0.0")
        families = {m.name: m for m in text_string_to_metric_families(output.decode())}
        self.assertIn('nh_roundtrip', families)
        parsed = [s for s in families['nh_roundtrip'].samples if s.native_histogram is not None]
        self.assertEqual(1, len(parsed))
        self.assertEqual(expected, parsed[0].native_histogram)

    def test_exposition_output_format(self):
        h = NativeHistogramMetric('nh_format', 'A native histogram', schema=0, registry=self.registry)
        h.observe(2)
        h.observe(-2)
        output = generate_latest(self.registry, version="2.0.0").decode()
        self.assertIn('# TYPE nh_format histogram\n', output)
        self.assertIn(
            'nh_format {count:2,sum:0.0,schema:0,zero_threshold:2.938735877055719e-39,zero_count:0,'
            'negative_spans:[1:1],negative_deltas:[1],positive_spans:[1:1],positive_deltas:[1]}\n',
            output)

    def test_concurrent_observe_and_collect(self):
        h = NativeHistogramMetric('nh', 'doc', schema=0, max_buckets=32, registry=self.registry)
        errors = []
        stop = threading.Event()

        def observer(values):
            for v in values:
                h.observe(v)

        def collector():
            while not stop.is_set():
                nh = get_native_histogram(h)
                total = nh.zero_count
                total += sum(decode_buckets(nh.pos_spans, nh.pos_deltas).values())
                total += sum(decode_buckets(nh.neg_spans, nh.neg_deltas).values())
                if total != nh.count_value:
                    errors.append((total, nh.count_value))

        observations = [2.0 ** (i % 20 - 10) * (1 if i % 2 else -1) for i in range(2000)]
        threads = [threading.Thread(target=observer, args=(observations[i::4],)) for i in range(4)]
        collector_thread = threading.Thread(target=collector)
        collector_thread.start()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        stop.set()
        collector_thread.join()

        self.assertEqual([], errors)
        nh = get_native_histogram(h)
        self.assertEqual(len(observations), nh.count_value)
        total = nh.zero_count
        total += sum(decode_buckets(nh.pos_spans, nh.pos_deltas).values())
        total += sum(decode_buckets(nh.neg_spans, nh.neg_deltas).values())
        self.assertEqual(len(observations), total)


if __name__ == '__main__':
    unittest.main()
