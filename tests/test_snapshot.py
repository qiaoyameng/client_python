import math
import unittest

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import Metric
from prometheus_client.snapshot import (
    collect_snapshot, FAIL, generate_latest_strict, SKIP, Snapshot,
    SnapshotValidationError,
)


class InMemoryCollector:
    """A collector returning a fixed, possibly invalid, set of metrics."""

    def __init__(self, metrics, name='InMemoryCollector'):
        self._metrics = metrics
        self.calls = 0

    def collect(self):
        self.calls += 1
        return self._metrics


class RaisingCollector:
    def __init__(self):
        self.calls = 0

    def collect(self):
        self.calls += 1
        raise RuntimeError('boom')


def gauge_family(name, value=1.0, labels=None):
    metric = Metric(name, 'help', 'gauge')
    metric.add_sample(name, labels or {}, value)
    return metric


def histogram_family(name, buckets, count=None, labels=None):
    metric = Metric(name, 'help', 'histogram')
    base = labels or {}
    for le, value in buckets:
        metric.add_sample(name + '_bucket', dict(base, le=le), value)
    if count is not None:
        metric.add_sample(name + '_count', dict(base), count)
    return metric


def summary_family(name, quantiles, count=3, sum_value=7.0, labels=None):
    metric = Metric(name, 'help', 'summary')
    base = labels or {}
    for quantile, value in quantiles:
        metric.add_sample(name, dict(base, quantile=quantile), value)
    metric.add_sample(name + '_count', dict(base), count)
    metric.add_sample(name + '_sum', dict(base), sum_value)
    return metric


class TestCollectOnce(unittest.TestCase):
    def test_each_collector_called_exactly_once(self):
        registry = CollectorRegistry()
        collectors = [InMemoryCollector([gauge_family(f'g{i}')]) for i in range(3)]
        for collector in collectors:
            registry.register(collector)
        snapshot = collect_snapshot(registry)
        self.assertEqual([1, 1, 1], [c.calls for c in collectors])
        self.assertEqual(3, len(snapshot.metrics))
        self.assertEqual((), snapshot.errors)

    def test_snapshot_matches_fast_export(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([
            gauge_family('temperature', 21.5, {'room': 'kitchen'}),
            histogram_family('latency', [('0.1', 1), ('1', 3), ('+Inf', 4)], count=4),
            summary_family('sizes', [('0.5', 2.0)], count=3, sum_value=7.0),
        ]))
        self.assertEqual(generate_latest(registry), generate_latest_strict(registry))

    def test_input_metrics_not_modified(self):
        metric = histogram_family('h', [('1', 2), ('+Inf', 2)], count=2)
        samples_before = list(metric.samples)
        snapshot = collect_snapshot(InMemoryCollector([metric]))
        self.assertEqual(samples_before, metric.samples)
        self.assertIs(snapshot.metrics[0], metric)

    def test_invalid_on_error_rejected(self):
        with self.assertRaises(ValueError):
            collect_snapshot(InMemoryCollector([]), on_error='ignore')


class TestSpecialFloats(unittest.TestCase):
    def test_special_floats_preserved(self):
        metric = Metric('special', 'help', 'gauge')
        metric.add_sample('special', {'kind': 'nan'}, float('nan'))
        metric.add_sample('special', {'kind': 'pinf'}, float('inf'))
        metric.add_sample('special', {'kind': 'ninf'}, float('-inf'))
        output = generate_latest_strict(InMemoryCollector([metric])).decode()
        self.assertIn('NaN', output)
        self.assertIn('+Inf', output)
        self.assertIn('-Inf', output)

    def test_non_numeric_value_rejected(self):
        metric = Metric('bad', 'help', 'gauge')
        metric.add_sample('bad', {}, 'not-a-number')
        with self.assertRaises(SnapshotValidationError):
            collect_snapshot(InMemoryCollector([metric]))


class TestFamilyConflicts(unittest.TestCase):
    def test_duplicate_family_name(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([gauge_family('dup', 1)]))
        registry.register(InMemoryCollector([gauge_family('dup', 2)]))
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(registry)
        self.assertIn('dup', str(ctx.exception))
        self.assertIn('collector=', str(ctx.exception))

    def test_family_type_conflict(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([gauge_family('conflict')]))
        counter = Metric('conflict', 'help', 'counter')
        counter.add_sample('conflict_total', {}, 1)
        registry.register(InMemoryCollector([counter]))
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(registry)
        self.assertIn('conflicting types', str(ctx.exception))

    def test_sample_name_collides_with_other_family(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([gauge_family('foo_total')]))
        counter = Metric('foo', 'help', 'counter')
        counter.add_sample('foo_total', {}, 1)
        registry.register(InMemoryCollector([counter]))
        with self.assertRaises(SnapshotValidationError):
            collect_snapshot(registry)


class TestDuplicateSamples(unittest.TestCase):
    def test_duplicate_sample_same_labels(self):
        metric = Metric('dup_sample', 'help', 'gauge')
        metric.add_sample('dup_sample', {'a': '1'}, 1)
        metric.add_sample('dup_sample', {'a': '1'}, 2)
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('dup_sample', str(ctx.exception))
        self.assertIn("'a': '1'", str(ctx.exception))

    def test_same_labels_different_values_still_duplicate(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([gauge_family('x', 1, {'a': 'b'})]))
        registry.register(InMemoryCollector([gauge_family('y', 2, {'a': 'b'})]))
        # Different family/sample names with same labels are fine.
        snapshot = collect_snapshot(registry)
        self.assertEqual(2, len(snapshot.metrics))


class TestLabelConflicts(unittest.TestCase):
    def test_le_label_on_non_bucket_sample(self):
        metric = Metric('h', 'help', 'histogram')
        metric.add_sample('h_bucket', {'le': '1'}, 1)
        metric.add_sample('h_bucket', {'le': '+Inf'}, 1)
        metric.add_sample('h_count', {'le': 'oops'}, 1)
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('"le"', str(ctx.exception))

    def test_quantile_label_on_summary_count(self):
        metric = Metric('s', 'help', 'summary')
        metric.add_sample('s', {'quantile': '0.5'}, 1)
        metric.add_sample('s_count', {'quantile': '0.5'}, 1)
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('"quantile"', str(ctx.exception))

    def test_label_names_collide_after_escaping(self):
        metric = Metric('esc_labels', 'help', 'gauge')
        metric.add_sample('esc_labels', {'a.b': '1', 'a_b': '2'}, 1)
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('collide after escaping', str(ctx.exception))


class TestHistogramValidation(unittest.TestCase):
    def test_non_monotonic_counts(self):
        # Cumulative bucket counts must be non-decreasing.
        metric = histogram_family('h', [('0.5', 3), ('1', 2), ('+Inf', 4)], count=4)
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('non-decreasing', str(ctx.exception))

    def test_duplicate_boundary(self):
        metric = histogram_family('h', [('1', 1), ('1', 2), ('+Inf', 3)], count=3)
        with self.assertRaises(SnapshotValidationError):
            collect_snapshot(InMemoryCollector([metric]))

    def test_missing_inf_bucket(self):
        metric = histogram_family('h', [('0.5', 1), ('1', 2)], count=2)
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('+Inf', str(ctx.exception))

    def test_count_mismatch(self):
        metric = histogram_family('h', [('1', 1), ('+Inf', 5)], count=3)
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('does not match', str(ctx.exception))

    def test_valid_histogram_passes(self):
        metric = histogram_family('h', [('0.5', 1), ('1', 2), ('+Inf', 4)], count=4)
        snapshot = collect_snapshot(InMemoryCollector([metric]))
        self.assertEqual(1, len(snapshot.metrics))


class TestSummaryValidation(unittest.TestCase):
    def test_quantile_out_of_range(self):
        metric = summary_family('s', [('1.5', 2.0)])
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(InMemoryCollector([metric]))
        self.assertIn('quantile', str(ctx.exception))

    def test_negative_quantile(self):
        metric = summary_family('s', [('-0.1', 2.0)])
        with self.assertRaises(SnapshotValidationError):
            collect_snapshot(InMemoryCollector([metric]))

    def test_valid_summary_passes(self):
        metric = summary_family('s', [('0', 1.0), ('0.5', 2.0), ('1', 3.0)])
        snapshot = collect_snapshot(InMemoryCollector([metric]))
        self.assertEqual(1, len(snapshot.metrics))


class TestEscapingCollisions(unittest.TestCase):
    def test_family_names_collide_after_escaping(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([gauge_family('foo.bar')]))
        registry.register(InMemoryCollector([gauge_family('foo_bar')]))
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(registry, escaping='underscores')
        self.assertIn('collide after escaping', str(ctx.exception))

    def test_no_collision_with_allow_utf8(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([gauge_family('foo.bar')]))
        registry.register(InMemoryCollector([gauge_family('foo_bar')]))
        snapshot = collect_snapshot(registry, escaping='allow-utf-8')
        self.assertEqual(2, len(snapshot.metrics))


class TestStrategies(unittest.TestCase):
    def _registry(self):
        registry = CollectorRegistry()
        registry.register(InMemoryCollector([gauge_family('good', 42)]))
        bad = histogram_family('bad_hist', [('1', 1)], count=1)  # missing +Inf
        registry.register(InMemoryCollector([bad]))
        return registry

    def test_fail_strategy_raises_with_context(self):
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(self._registry(), on_error=FAIL)
        message = str(ctx.exception)
        self.assertIn('collector=', message)
        self.assertIn('family=bad_hist', message)

    def test_skip_strategy_isolates_bad_family(self):
        snapshot = collect_snapshot(self._registry(), on_error=SKIP)
        self.assertEqual(['good'], [m.name for m in snapshot.metrics])
        self.assertEqual(1, len(snapshot.errors))
        self.assertEqual('bad_hist', snapshot.errors[0].metric)
        self.assertIsNotNone(snapshot.errors[0].collector)
        output = generate_latest(snapshot).decode()
        self.assertIn('good 42.0', output)
        self.assertNotIn('bad_hist', output)

    def test_raising_collector_fail(self):
        registry = CollectorRegistry()
        registry.register(RaisingCollector())
        with self.assertRaises(SnapshotValidationError) as ctx:
            collect_snapshot(registry, on_error=FAIL)
        self.assertIn('boom', str(ctx.exception))

    def test_raising_collector_skip(self):
        registry = CollectorRegistry()
        raising = RaisingCollector()
        registry.register(raising)
        registry.register(InMemoryCollector([gauge_family('good', 1)]))
        snapshot = collect_snapshot(registry, on_error=SKIP)
        self.assertEqual(1, raising.calls)
        self.assertEqual(['good'], [m.name for m in snapshot.metrics])
        self.assertEqual(1, len(snapshot.errors))

    def test_generate_latest_strict_skip(self):
        output = generate_latest_strict(self._registry(), on_error=SKIP).decode()
        self.assertIn('good 42.0', output)
        self.assertNotIn('bad_hist', output)


if __name__ == '__main__':
    unittest.main()
