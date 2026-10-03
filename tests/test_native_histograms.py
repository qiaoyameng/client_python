import unittest

from prometheus_client import core
from prometheus_client.native_histograms import (
    NativeHistogramClassicCollector,
    estimate_native_histogram_bounds,
    project_native_histogram_bounds,
)
from prometheus_client.samples import BucketSpan, NativeHistogram


def histogram(count=10, zero_threshold=0.0, zero_count=1, pos_deltas=(2, 1),
              neg_deltas=(4,)):
    return NativeHistogram(
        count_value=count,
        sum_value=12.5,
        schema=0,
        zero_threshold=zero_threshold,
        zero_count=zero_count,
        pos_spans=(BucketSpan(0, 1), BucketSpan(1, 1)),
        neg_spans=(BucketSpan(0, 1),),
        pos_deltas=pos_deltas,
        neg_deltas=neg_deltas,
    )


class NativeHistogramCollector:
    def collect(self):
        metric = core.HistogramMetricFamily('request_duration_seconds', 'help')
        metric.add_sample(
            'request_duration_seconds',
            {'route': '/users'},
            None,
            99,
            native_histogram=histogram(),
        )
        metric.add_sample('request_duration_seconds_created', {'route': '/users'}, 123, 98)
        yield metric


class TestNativeHistogramProjection(unittest.TestCase):
    def test_decodes_deltas_sparse_positive_and_negative_buckets(self):
        projection = project_native_histogram_bounds(
            histogram(),
            (0, 2, float('inf')),
            created=123,
        )

        self.assertEqual(projection.bounds, (0, 2, float('inf')))
        self.assertEqual(projection.cumulative_counts, (5, 7, 10))
        self.assertEqual(projection.count, 10)
        self.assertEqual(projection.sum_value, 12.5)
        self.assertEqual(projection.created, 123)

    def test_zero_threshold_is_an_exact_non_negative_boundary(self):
        native = NativeHistogram(
            count_value=9,
            sum_value=7,
            schema=0,
            zero_threshold=1,
            zero_count=2,
            pos_spans=(BucketSpan(1, 1),),
            neg_spans=(BucketSpan(0, 1),),
            pos_deltas=(3,),
            neg_deltas=(4,),
        )

        projection = project_native_histogram_bounds(native, (1, float('inf')))

        self.assertEqual(projection.cumulative_counts, (6, 9))

    def test_boundary_inside_occupied_or_empty_bucket_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Boundary 1.5'):
            project_native_histogram_bounds(histogram(), (1.5, 2, float('inf')))

        native = NativeHistogram(
            count_value=5,
            sum_value=20,
            schema=0,
            zero_threshold=0,
            zero_count=1,
            pos_spans=(BucketSpan(0, 1), BucketSpan(2, 1)),
            pos_deltas=(0, 4),
        )

        with self.assertRaisesRegex(ValueError, 'Boundary 1.5'):
            project_native_histogram_bounds(native, (1.5, float('inf')))

    def test_interval_estimates_are_explicitly_marked(self):
        estimates = estimate_native_histogram_bounds(
            histogram(),
            (1, 2, 3, 4),
        )

        self.assertEqual(
            [(estimate.lower, estimate.upper, estimate.exact) for estimate in estimates],
            [(5, 5, True), (7, 7, True), (7, 7, False), (7, 7, True)],
        )

    def test_negative_boundaries_project_with_grid_alignment_and_explicit_estimates(self):
        estimates = estimate_native_histogram_bounds(
            histogram(),
            (float('-inf'), -2, -1, -0.5, 0, float('inf')),
        )

        self.assertEqual(
            [(estimate.lower, estimate.upper, estimate.exact) for estimate in estimates],
            [(0, 0, True), (0, 0, True), (4, 4, True), (4, 4, True), (5, 5, True),
             (10, 10, True)],
        )
        self.assertEqual(
            project_native_histogram_bounds(histogram(), (-1, 0, float('inf'))).cumulative_counts,
            (4, 5, 10),
        )

    def test_negative_zero_threshold_boundary_is_lossless(self):
        native = NativeHistogram(
            count_value=9,
            sum_value=7,
            schema=0,
            zero_threshold=1,
            zero_count=2,
            pos_spans=(BucketSpan(1, 1),),
            neg_spans=(BucketSpan(0, 1),),
            pos_deltas=(3,),
            neg_deltas=(4,),
        )

        projection = project_native_histogram_bounds(native, (-1, 1, float('inf')))

        self.assertEqual(projection.cumulative_counts, (2, 6, 9))

        with self.assertRaisesRegex(ValueError, 'Boundary 0.5'):
            project_native_histogram_bounds(native, (0.5, 1, float('inf')))

    def test_collector_preserves_labels_count_sum_created_and_timestamps(self):
        adapted = NativeHistogramClassicCollector(
            NativeHistogramCollector(),
            (0, 2),
        )
        metric = next(adapted.collect())

        samples = {(sample.name, tuple(sorted(sample.labels.items()))): sample
                   for sample in metric.samples}
        labels = (('route', '/users'),)

        def bucket_key(le):
            return ('request_duration_seconds_bucket', (('le', le),) + labels)
        self.assertEqual(
            samples[bucket_key('0.0')].value,
            5,
        )
        self.assertEqual(
            samples[bucket_key('+Inf')].value,
            10,
        )
        self.assertEqual(samples[('request_duration_seconds_count', labels)].value, 10)
        self.assertEqual(samples[('request_duration_seconds_sum', labels)].value, 12.5)
        self.assertEqual(samples[('request_duration_seconds_created', labels)].value, 123)
        self.assertEqual(
            samples[bucket_key('0.0')].timestamp,
            99,
        )
        self.assertEqual(
            samples[('request_duration_seconds_created', labels)].timestamp,
            98,
        )
        source_metric = next(NativeHistogramCollector().collect())
        self.assertNotEqual(metric.samples, source_metric.samples)
        self.assertEqual(len(source_metric.samples), 2)

    def test_collector_without_describe_or_created(self):
        class Collector:
            def collect(self):
                metric = core.HistogramMetricFamily('h', 'help')
                metric.add_sample('h', {}, None, native_histogram=histogram())
                yield metric

        adapted = NativeHistogramClassicCollector(Collector(), (0, 2))

        described = list(adapted.describe())
        collected = list(adapted.collect())
        self.assertEqual(described, collected)
        self.assertNotIn('h_created', {sample.name for sample in collected[0].samples})

    def test_collector_keeps_classic_groups_without_native_histograms(self):
        class Collector:
            def collect(self):
                metric = core.HistogramMetricFamily('h', 'help')
                metric.add_sample(
                    'h',
                    {'kind': 'native'},
                    None,
                    native_histogram=histogram(),
                )
                metric.add_sample('h_bucket', {'kind': 'classic', 'le': '2.0'}, 3)
                metric.add_sample('h_bucket', {'kind': 'classic', 'le': '+Inf'}, 4)
                metric.add_sample('h_count', {'kind': 'classic'}, 4)
                metric.add_sample('h_sum', {'kind': 'classic'}, 9)
                yield metric

        metric = next(NativeHistogramClassicCollector(Collector(), (0, 2)).collect())

        values = {(sample.name, tuple(sorted(sample.labels.items()))): sample.value
                  for sample in metric.samples}
        self.assertEqual(values[('h_bucket', (('kind', 'classic'), ('le', '2.0')))], 3)
        self.assertEqual(values[('h_bucket', (('kind', 'classic'), ('le', '+Inf')))], 4)
        self.assertEqual(values[('h_count', (('kind', 'classic'),))], 4)
        self.assertEqual(values[('h_bucket', (('kind', 'native'), ('le', '+Inf')))], 10)

    def test_rejects_negative_decoded_bucket_count(self):
        with self.assertRaisesRegex(ValueError, 'positive bucket count'):
            project_native_histogram_bounds(histogram(pos_deltas=(-2, 1)), (0, float('inf')))

    def test_rejects_inconsistent_total(self):
        with self.assertRaisesRegex(ValueError, 'does not match bucket totals'):
            project_native_histogram_bounds(histogram(count=11), (0, float('inf')))

    def test_rejects_invalid_span_and_does_not_modify_input(self):
        native = histogram()
        invalid = NativeHistogram(
            native.count_value,
            native.sum_value,
            native.schema,
            native.zero_threshold,
            native.zero_count,
            (BucketSpan(0, 1), BucketSpan(-1, 1)),
            native.neg_spans,
            (2, 1),
            native.neg_deltas,
        )

        with self.assertRaisesRegex(ValueError, 'positive span 1 has an invalid offset'):
            project_native_histogram_bounds(invalid, (0, float('inf')))
        self.assertEqual(invalid.pos_deltas, (2, 1))

    def test_rejects_positive_bucket_completely_covered_by_zero_threshold(self):
        native = NativeHistogram(
            count_value=4,
            sum_value=1,
            schema=0,
            zero_threshold=2,
            zero_count=1,
            pos_spans=(BucketSpan(0, 1),),
            pos_deltas=(3,),
        )

        with self.assertRaisesRegex(ValueError, 'positive bucket 0 overlaps'):
            project_native_histogram_bounds(native, (2, float('inf')))


if __name__ == '__main__':
    unittest.main()
