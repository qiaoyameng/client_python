import unittest

from prometheus_client import (
    CollectorRegistry, generate_latest, NativeHistogramClassicAdapter,
)
from prometheus_client.metrics_core import Metric
from prometheus_client.samples import BucketSpan, NativeHistogram, Sample
from prometheus_client.utils import floatToGoString


class StaticSource:
    """A minimal collector returning a fixed set of metric families."""

    def __init__(self, metrics):
        self._metrics = metrics

    def collect(self):
        return self._metrics


def make_native_histogram(**overrides):
    """A sparse native histogram with schema 0 (base 2).

    Positive buckets: (1, 2] count 2, (2, 4] count 3, (8, 16] count 5.
    Negative buckets: [-4, -2) count 1, [-8, -4) count 4.
    Zero bucket: [-0.5, 0.5] count 1.
    """
    values = {
        'count_value': 16.0,
        'sum_value': -2.5,
        'schema': 0,
        'zero_threshold': 0.5,
        'zero_count': 1.0,
        'pos_spans': [BucketSpan(1, 2), BucketSpan(1, 1)],
        'pos_deltas': [2, 1, 2],
        'neg_spans': [BucketSpan(2, 1), BucketSpan(0, 1)],
        'neg_deltas': [1, 3],
    }
    values.update(overrides)
    return NativeHistogram(**values)


def make_family(name='latency', labels=None, native_histogram=None, created=None, timestamp=None):
    metric = Metric(name, 'A native histogram.', 'histogram')
    if native_histogram is None:
        native_histogram = make_native_histogram()
    metric.samples.append(Sample(name, labels, None, timestamp, None, native_histogram))
    if created is not None:
        metric.samples.append(Sample(name + '_created', labels, created, timestamp))
    return metric


def bucket_values(metric, name='latency'):
    return {
        sample.labels['le']: sample.value
        for sample in metric.samples
        if sample.name == name + '_bucket'
    }


def sample_values(metric, name):
    return [sample.value for sample in metric.samples if sample.name == name]


class TestNativeHistogramClassicAdapter(unittest.TestCase):
    def test_exact_aligned_projection_is_lossless(self):
        labels = {'method': 'get'}
        source = StaticSource([make_family(labels=labels)])
        adapter = NativeHistogramClassicAdapter(
            source, {'latency': [-16, -2, -1.5, 0.5, 2, 4, 16]})

        (classic,) = list(adapter.collect())

        self.assertEqual('latency', classic.name)
        self.assertEqual('histogram', classic.type)
        self.assertEqual('A native histogram.', classic.documentation)
        expected = {-16: 0, -2: 5, -1.5: 5, 0.5: 6, 2: 8, 4: 11, 16: 16}
        expected[float('inf')] = 16
        self.assertEqual(
            {floatToGoString(le): value for le, value in expected.items()},
            bucket_values(classic))
        # The +Inf bucket must match the count.
        self.assertEqual([16.0], sample_values(classic, 'latency_count'))
        self.assertEqual([-2.5], sample_values(classic, 'latency_sum'))
        # Labels are preserved on every produced sample.
        for sample in classic.samples:
            self.assertEqual('get', sample.labels['method'])

    def test_exact_mode_rejects_boundaries_cutting_buckets(self):
        # 3 lies inside (2, 4]; -4 is the inclusive lower edge of [-4, -2);
        # 0.2 lies inside the zero bucket [-0.5, 0.5].
        for le in (3, -4, 0.2):
            adapter = NativeHistogramClassicAdapter(
                StaticSource([make_family()]), {'latency': [le]})
            with self.assertRaises(ValueError):
                list(adapter.collect())

    def test_exact_mode_allows_cutting_empty_buckets(self):
        # The (4, 8] bucket is empty, so a boundary at 8 cutting it is exact.
        native_histogram = make_native_histogram(
            count_value=10.0,
            zero_threshold=0.0,
            zero_count=0.0,
            pos_spans=[BucketSpan(1, 4)],
            pos_deltas=[2, 1, -3, 5],
            neg_spans=None,
            neg_deltas=None,
        )
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family(native_histogram=native_histogram)]),
            {'latency': [8]})
        (classic,) = list(adapter.collect())
        self.assertEqual({'8.0': 5.0, '+Inf': 10.0}, bucket_values(classic))

    def test_estimate_mode_reports_labelled_intervals(self):
        source = StaticSource([make_family(labels={'method': 'get'})])
        adapter = NativeHistogramClassicAdapter(
            source, {'latency': [3, 16]}, mode='estimate')

        classic, estimates = list(adapter.collect())

        # Aligned boundaries stay exact in the classic histogram.
        self.assertEqual({'16.0': 16.0, '+Inf': 16.0}, bucket_values(classic))
        self.assertEqual([16.0], sample_values(classic, 'latency_count'))
        self.assertEqual([-2.5], sample_values(classic, 'latency_sum'))
        # The cut boundary becomes a clearly labelled interval estimate.
        self.assertEqual('latency_bucket_estimate', estimates.name)
        self.assertEqual('gauge', estimates.type)
        self.assertIn('estimate', estimates.documentation.lower())
        by_bound = {
            sample.labels['bound']: sample.value
            for sample in estimates.samples
            if sample.labels['le'] == '3.0'
        }
        self.assertEqual({'lower': 8.0, 'upper': 11.0}, by_bound)
        for sample in estimates.samples:
            self.assertEqual('get', sample.labels['method'])

    def test_zero_bucket_cut_estimate(self):
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family()]), {'latency': [0.2]}, mode='estimate')
        _, estimates = list(adapter.collect())
        by_bound = {sample.labels['bound']: sample.value for sample in estimates.samples}
        # Everything below the zero bucket is exact; the zero count of 1 is
        # the uncertain part.
        self.assertEqual({'lower': 5.0, 'upper': 6.0}, by_bound)

    def test_created_and_timestamp_are_preserved(self):
        source = StaticSource([make_family(created=123.0, timestamp=456.0)])
        adapter = NativeHistogramClassicAdapter(source, {'latency': [2]})
        (classic,) = list(adapter.collect())

        created = [s for s in classic.samples if s.name == 'latency_created']
        self.assertEqual(1, len(created))
        self.assertEqual(123.0, created[0].value)
        self.assertEqual(456.0, created[0].timestamp)
        for sample in classic.samples:
            self.assertEqual(456.0, sample.timestamp)

    def test_source_samples_are_not_modified(self):
        labels = {'method': 'get'}
        family = make_family(labels=labels, created=123.0)
        original_samples = list(family.samples)
        adapter = NativeHistogramClassicAdapter(
            StaticSource([family]), {'latency': [2, 4]})
        list(adapter.collect())

        self.assertEqual(original_samples, family.samples)
        self.assertEqual({'method': 'get'}, labels)
        for sample in family.samples:
            self.assertNotIn('le', sample.labels)
            if sample.name == 'latency':
                self.assertIsNotNone(sample.native_histogram)

    def test_families_without_native_histograms_pass_through(self):
        plain = Metric('temperature', 'A gauge.', 'gauge')
        plain.samples.append(Sample('temperature', {'room': 'kitchen'}, 21.0))
        source = StaticSource([plain])
        adapter = NativeHistogramClassicAdapter(source, [0, 1])
        self.assertEqual([plain], list(adapter.collect()))

    def test_unconfigured_families_pass_through(self):
        family = make_family()
        adapter = NativeHistogramClassicAdapter(
            StaticSource([family]), {'other_metric': [1, 2]})
        self.assertEqual([family], list(adapter.collect()))

    def test_sequence_boundaries_apply_to_all_families(self):
        source = StaticSource([make_family('one'), make_family('two')])
        adapter = NativeHistogramClassicAdapter(source, [2, 4])
        one, two = list(adapter.collect())
        self.assertEqual({'2.0': 8.0, '4.0': 11.0, '+Inf': 16.0}, bucket_values(one, 'one'))
        self.assertEqual({'2.0': 8.0, '4.0': 11.0, '+Inf': 16.0}, bucket_values(two, 'two'))

    def test_empty_histogram(self):
        native_histogram = make_native_histogram(
            count_value=5.0,
            sum_value=0.0,
            zero_threshold=0.0,
            zero_count=5.0,
            pos_spans=[BucketSpan(3, 0)],
            pos_deltas=[],
            neg_spans=None,
            neg_deltas=None,
        )
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family(native_histogram=native_histogram)]),
            {'latency': [-1, 0, 1]})
        (classic,) = list(adapter.collect())
        self.assertEqual(
            {'-1.0': 0.0, '0.0': 5.0, '1.0': 5.0, '+Inf': 5.0},
            bucket_values(classic))

    def test_negative_bucket_cumulative_direction(self):
        # Counts must accumulate from the most negative bucket upwards;
        # negative bucket indexes must not be sorted as plain numbers.
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family()]), {'latency': [-16, -2, 0.5]})
        (classic,) = list(adapter.collect())
        self.assertEqual(
            {'-16.0': 0.0, '-2.0': 5.0, '0.5': 6.0, '+Inf': 16.0},
            bucket_values(classic))

    def test_invalid_spans_are_rejected(self):
        # Span covers two buckets but only one delta is given.
        bad = make_native_histogram(pos_spans=[BucketSpan(1, 2)], pos_deltas=[1])
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family(native_histogram=bad)]), {'latency': [2]})
        with self.assertRaises(ValueError):
            list(adapter.collect())

        # More deltas than spanned buckets.
        bad = make_native_histogram(pos_spans=[BucketSpan(1, 1)], pos_deltas=[1, 1])
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family(native_histogram=bad)]), {'latency': [2]})
        with self.assertRaises(ValueError):
            list(adapter.collect())

        # Negative span length.
        bad = make_native_histogram(pos_spans=[BucketSpan(1, -1)], pos_deltas=[])
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family(native_histogram=bad)]), {'latency': [2]})
        with self.assertRaises(ValueError):
            list(adapter.collect())

    def test_negative_bucket_count_is_rejected(self):
        bad = make_native_histogram(pos_spans=[BucketSpan(1, 2)], pos_deltas=[1, -5])
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family(native_histogram=bad)]), {'latency': [2]})
        with self.assertRaises(ValueError):
            list(adapter.collect())

    def test_inconsistent_total_is_rejected(self):
        bad = make_native_histogram(count_value=15.0)
        adapter = NativeHistogramClassicAdapter(
            StaticSource([make_family(native_histogram=bad)]), {'latency': [2]})
        with self.assertRaises(ValueError):
            list(adapter.collect())

    def test_invalid_schema_is_rejected(self):
        for schema in (9, -5, -53):
            bad = make_native_histogram(schema=schema)
            adapter = NativeHistogramClassicAdapter(
                StaticSource([make_family(native_histogram=bad)]), {'latency': [2]})
            with self.assertRaises(ValueError):
                list(adapter.collect())

    def test_boundary_configuration_is_validated(self):
        source = StaticSource([])
        for buckets in ([2, 1], [2, 2], [float('inf')], [float('nan')], []):
            with self.assertRaises(ValueError):
                NativeHistogramClassicAdapter(source, buckets)
        with self.assertRaises(ValueError):
            NativeHistogramClassicAdapter(source, {'latency': [4, 2]})
        with self.assertRaises(ValueError):
            NativeHistogramClassicAdapter(source, [1, 2], mode='precise')

    def test_registry_integration(self):
        registry = CollectorRegistry()
        source = StaticSource([make_family(labels={'method': 'get'}, created=123.0)])
        registry.register(NativeHistogramClassicAdapter(source, {'latency': [2, 4]}))

        output = generate_latest(registry).decode()

        self.assertIn('# TYPE latency histogram', output)
        self.assertIn('latency_bucket{le="2.0",method="get"} 8.0', output)
        self.assertIn('latency_bucket{le="+Inf",method="get"} 16.0', output)
        self.assertIn('latency_count{method="get"} 16.0', output)
        self.assertIn('latency_sum{method="get"} -2.5', output)
        self.assertIn('latency_created{method="get"} 123.0', output)


if __name__ == '__main__':
    unittest.main()
