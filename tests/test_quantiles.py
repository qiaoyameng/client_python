import bisect
import math
import os
import random
import shutil
import tempfile
from threading import Thread
import unittest

from prometheus_client import CollectorRegistry, Summary, values
from prometheus_client.exposition import generate_latest
from prometheus_client.quantile import Estimator, TimeWindowQuantiles
from prometheus_client.values import MultiProcessValue, MutexValue


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def assert_rank_within_error(test_case, data, quantile, error, value):
    """Assert the exact rank of `value` in `data` is within error of quantile."""
    data = sorted(data)
    n = len(data)
    lo = bisect.bisect_left(data, value)
    hi = bisect.bisect_right(data, value)
    # +1 tolerates the discretization of ranks for small n.
    test_case.assertTrue(
        any(abs(rank - quantile * n) <= error * n + 1 for rank in range(lo, hi)),
        '%r (rank %d..%d) is not within %r of ideal rank %r (n=%d)' % (
            value, lo, hi, error * n, quantile * n, n),
    )


class TestEstimator(unittest.TestCase):
    def test_rank_error_against_exact_sort(self):
        invariants = [(0.5, 0.05), (0.9, 0.01), (0.99, 0.005)]
        random.seed(1234)
        data = [random.uniform(-100, 100) for _ in range(2000)]
        estimator = Estimator(invariants)
        for value in data:
            estimator.observe(value)
        for quantile, error in invariants:
            assert_rank_within_error(self, data, quantile, error, estimator.query(quantile))

    def test_memory_is_bounded(self):
        estimator = Estimator([(0.5, 0.05), (0.9, 0.01), (0.99, 0.01)])
        random.seed(1234)
        for _ in range(50000):
            estimator.observe(random.random())
        estimator._flush()
        self.assertLess(len(estimator._values), 50000 // 10)

    def test_negative_and_duplicate_values(self):
        data = [-5.0, -5.0, -1.0, 0.0, 0.0, 0.0, 2.5, 2.5, 7.0, 7.0] * 100
        random.seed(1234)
        random.shuffle(data)
        estimator = Estimator([(0.5, 0.02), (0.9, 0.02)])
        for value in data:
            estimator.observe(value)
        assert_rank_within_error(self, data, 0.5, 0.02, estimator.query(0.5))
        assert_rank_within_error(self, data, 0.9, 0.02, estimator.query(0.9))

    def test_extremes_are_exact(self):
        estimator = Estimator([(0.5, 0.05)])
        for value in (3.0, -1.0, 42.0, 0.0):
            estimator.observe(value)
        self.assertEqual(-1.0, estimator.query(0.0))
        self.assertEqual(42.0, estimator.query(1.0))

    def test_empty_query_is_nan(self):
        estimator = Estimator([(0.5, 0.05)])
        self.assertTrue(math.isnan(estimator.query(0.5)))

    def test_invalid_invariants_rejected(self):
        with self.assertRaises(ValueError):
            Estimator([])
        with self.assertRaises(ValueError):
            Estimator([(0.0, 0.05)])
        with self.assertRaises(ValueError):
            Estimator([(1.0, 0.05)])
        with self.assertRaises(ValueError):
            Estimator([(0.5, 0.0)])
        with self.assertRaises(ValueError):
            Estimator([(0.5, 1.0)])


class TestTimeWindowQuantiles(unittest.TestCase):
    def make_window(self, max_age=10.0, age_buckets=5):
        clock = FakeClock()
        window = TimeWindowQuantiles(
            [(0.5, 0.05), (0.9, 0.01)], max_age=max_age,
            age_buckets=age_buckets, clock=clock)
        return window, clock

    def test_exposes_window_bounds(self):
        window, _ = self.make_window(max_age=10.0, age_buckets=5)
        self.assertEqual(10.0, window.max_age)
        self.assertEqual(5, window.age_buckets)
        self.assertEqual(2.0, window.max_age_error)

    def test_observations_expire(self):
        window, clock = self.make_window()
        window.observe(1.0)
        self.assertEqual(1.0, window.snapshot()[0.5])
        # Still visible just before max_age.
        clock.advance(9.9)
        self.assertEqual(1.0, window.snapshot()[0.5])
        # Expired once max_age has passed.
        clock.advance(0.2)
        self.assertTrue(math.isnan(window.snapshot()[0.5]))

    def test_observation_lifetime_within_documented_bounds(self):
        window, clock = self.make_window(max_age=10.0, age_buckets=5)
        window.observe(1.0)
        # Lifetime is at least max_age - max_age_error.
        clock.advance(8.0)
        self.assertEqual(1.0, window.snapshot()[0.5])
        # And at most max_age.
        clock.advance(2.1)
        self.assertTrue(math.isnan(window.snapshot()[0.5]))

    def test_clock_crossing_multiple_slices_at_once(self):
        window, clock = self.make_window()
        window.observe(1.0)
        clock.advance(6.3)  # Crosses three 2s slices in one step.
        self.assertEqual(1.0, window.snapshot()[0.5])
        clock.advance(25.0)  # Crosses the whole window in one step.
        self.assertTrue(math.isnan(window.snapshot()[0.5]))

    def test_window_reflects_only_recent_observations(self):
        window, clock = self.make_window()
        for _ in range(100):
            window.observe(1000.0)
        clock.advance(11.0)
        for _ in range(100):
            window.observe(2.0)
        snapshot = window.snapshot()
        self.assertEqual(2.0, snapshot[0.5])
        self.assertEqual(2.0, snapshot[0.9])

    def test_snapshot_is_consistent_across_quantiles(self):
        window, _ = self.make_window()
        random.seed(1234)
        for _ in range(1000):
            window.observe(random.random())
        snapshot = window.snapshot()
        self.assertEqual({0.5, 0.9}, set(snapshot))
        self.assertLessEqual(snapshot[0.5], snapshot[0.9])

    def test_empty_window_is_nan(self):
        window, _ = self.make_window()
        snapshot = window.snapshot()
        self.assertTrue(math.isnan(snapshot[0.5]))
        self.assertTrue(math.isnan(snapshot[0.9]))

    def test_backwards_clock_does_not_corrupt(self):
        window, clock = self.make_window()
        window.observe(1.0)
        clock.advance(-100.0)
        self.assertEqual(1.0, window.snapshot()[0.5])

    def test_invalid_config_rejected(self):
        with self.assertRaises(ValueError):
            TimeWindowQuantiles([(0.5, 0.05)], max_age=0)
        with self.assertRaises(ValueError):
            TimeWindowQuantiles([(0.5, 0.05)], max_age=10, age_buckets=0)


class TestSummaryQuantiles(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        self.clock = FakeClock()

    def make_summary(self, name='s', quantiles=((0.5, 0.05), (0.9, 0.01)), **kwargs):
        kwargs.setdefault('max_age', 10.0)
        kwargs.setdefault('age_buckets', 5)
        kwargs.setdefault('clock', self.clock)
        return Summary(name, 'help', quantiles=quantiles, registry=self.registry, **kwargs)

    def test_quantile_samples_exposed(self):
        summary = self.make_summary()
        for value in (1.0, 2.0, 3.0, 4.0, 5.0):
            summary.observe(value)
        median = self.registry.get_sample_value('s', {'quantile': '0.5'})
        p90 = self.registry.get_sample_value('s', {'quantile': '0.9'})
        self.assertTrue(1.0 <= median <= 5.0)
        self.assertTrue(median <= p90 <= 5.0)
        self.assertEqual(5, self.registry.get_sample_value('s_count'))
        self.assertEqual(15.0, self.registry.get_sample_value('s_sum'))

    def test_rank_error_against_exact_sort(self):
        summary = self.make_summary(quantiles=((0.5, 0.02), (0.9, 0.01), (0.99, 0.01)))
        random.seed(1234)
        data = [random.random() for _ in range(1000)]
        for value in data:
            summary.observe(value)
        for quantile, error in ((0.5, 0.02), (0.9, 0.01), (0.99, 0.01)):
            value = self.registry.get_sample_value('s', {'quantile': str(quantile)})
            assert_rank_within_error(self, data, quantile, error, value)

    def test_count_and_sum_survive_window_expiry(self):
        summary = self.make_summary()
        summary.observe(7.0)
        self.clock.advance(60.0)  # Expire the whole window.
        self.assertTrue(math.isnan(self.registry.get_sample_value('s', {'quantile': '0.5'})))
        # count/sum are cumulative and must not be reset by window rotation.
        self.assertEqual(1, self.registry.get_sample_value('s_count'))
        self.assertEqual(7.0, self.registry.get_sample_value('s_sum'))

    def test_empty_window_reports_nan(self):
        self.make_summary()
        self.assertTrue(math.isnan(self.registry.get_sample_value('s', {'quantile': '0.5'})))
        self.assertEqual(0, self.registry.get_sample_value('s_count'))

    def test_negative_and_duplicate_values(self):
        summary = self.make_summary()
        for value in (-3.0, -3.0, -1.0, 0.0, 2.0, 2.0):
            summary.observe(value)
        median = self.registry.get_sample_value('s', {'quantile': '0.5'})
        self.assertTrue(-3.0 <= median <= 2.0)
        self.assertEqual(-3.0, self.registry.get_sample_value('s_sum'))

    def test_non_finite_observations_rejected(self):
        summary = self.make_summary()
        for bad in (float('nan'), float('inf'), float('-inf')):
            with self.assertRaises(ValueError):
                summary.observe(bad)
        self.assertEqual(0, self.registry.get_sample_value('s_count'))
        self.assertEqual(0, self.registry.get_sample_value('s_sum'))

    def test_window_metadata_exposed(self):
        summary = self.make_summary(max_age=30.0, age_buckets=6)
        self.assertEqual(30.0, summary.max_age)
        self.assertEqual(6, summary.age_buckets)
        self.assertEqual(5.0, summary.max_age_error)

    def test_disabled_by_default(self):
        summary = Summary('plain', 'help', registry=self.registry)
        summary.observe(1.0)
        self.assertIsNone(summary.max_age)
        self.assertIsNone(summary.age_buckets)
        self.assertIsNone(summary.max_age_error)
        self.assertIsNone(self.registry.get_sample_value('plain', {'quantile': '0.5'}))
        self.assertEqual(1, self.registry.get_sample_value('plain_count'))

    def test_labels_children_share_config_and_clock(self):
        summary = Summary('labeled', 'help', labelnames=('path',), quantiles=[(0.5, 0.05)],
                          max_age=10.0, age_buckets=5, clock=self.clock, registry=self.registry)
        child = summary.labels('GET')
        child.observe(4.0)
        self.assertEqual(4.0, self.registry.get_sample_value('labeled', {'quantile': '0.5', 'path': 'GET'}))
        self.assertEqual(10.0, child.max_age)
        self.clock.advance(60.0)
        self.assertTrue(math.isnan(self.registry.get_sample_value('labeled', {'quantile': '0.5', 'path': 'GET'})))

    def test_exposition_format(self):
        summary = self.make_summary()
        summary.observe(1.5)
        output = generate_latest(self.registry).decode()
        self.assertIn('s{quantile="0.5"} 1.5', output)
        self.assertIn('s{quantile="0.9"} 1.5', output)
        self.assertIn('s_count 1.0', output)
        self.assertIn('s_sum 1.5', output)

    def test_concurrent_observe_and_collect(self):
        summary = self.make_summary()
        errors = []
        observations_per_thread = 2000

        def observe():
            try:
                for i in range(observations_per_thread):
                    summary.observe(float(i % 100))
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        def collect():
            try:
                for _ in range(200):
                    for metric in self.registry.collect():
                        for sample in metric.samples:
                            if sample.name == 's':
                                self.assertTrue(math.isfinite(sample.value))
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [Thread(target=observe) for _ in range(3)] + [Thread(target=collect)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual([], errors)
        self.assertEqual(3 * observations_per_thread, self.registry.get_sample_value('s_count'))


class TestSummaryQuantilesMultiprocess(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = self.tempdir
        values.ValueClass = MultiProcessValue(lambda: 123)

    def tearDown(self):
        os.environ.pop('PROMETHEUS_MULTIPROC_DIR', None)
        values.close_all_multiprocess_files()
        values.ValueClass = MutexValue
        shutil.rmtree(self.tempdir)

    def test_quantiles_rejected_in_multiprocess_mode(self):
        with self.assertRaises(ValueError):
            Summary('s', 'help', quantiles=[(0.5, 0.05)], registry=CollectorRegistry())

    def test_plain_summary_still_works_in_multiprocess_mode(self):
        registry = CollectorRegistry()
        summary = Summary('s', 'help', registry=registry)
        summary.observe(1.0)
        self.assertEqual(1, registry.get_sample_value('s_count'))


if __name__ == '__main__':
    unittest.main()
