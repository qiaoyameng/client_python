import unittest

from prometheus_client.core import CollectorRegistry
from prometheus_client.delta_collector import (
    BatchConflictError, BucketLayoutChangeError, CounterDelta, DeltaBatch,
    DeltaCollector, HistogramDelta, HistogramValidationError,
    IntervalGapError, IntervalOverlapError, InvalidDeltaError,
    OutOfOrderBatchError, StaleGenerationError,
)


def counter(name='requests', labels=None, value=1.0):
    return CounterDelta(name=name, labels=labels or {}, value=value)


def histogram(name='latency', labels=None, count=3, sum=1.5,
              buckets=((0.5, 2), (1.0, 1))):
    return HistogramDelta(name=name, labels=labels or {}, count=count,
                          sum=sum, buckets=tuple(buckets))


def batch(stream='agent-1', generation=1, sequence=0, start=0.0, end=10.0,
          updates=()):
    return DeltaBatch(stream_id=stream, generation=generation,
                      sequence=sequence, interval_start=start,
                      interval_end=end, updates=tuple(updates))


def samples(collector, name):
    for metric in collector.collect():
        if metric.name == name:
            return {s.name: s for s in metric.samples}
    return {}


def bucket_samples(collector, name):
    for metric in collector.collect():
        if metric.name == name:
            return [s for s in metric.samples if s.name == name + '_bucket']
    return []


class TestDeltaCollectorCounters(unittest.TestCase):
    def test_accumulates_fixed_sequence(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, start=0.0, end=10.0, updates=[counter(value=2.0)]))
        c.apply(batch(sequence=1, start=10.0, end=20.0, updates=[counter(value=3.0)]))
        c.apply(batch(sequence=2, start=20.0, end=30.0, updates=[counter(value=5.0)]))
        s = samples(c, 'requests')
        self.assertEqual(10.0, s['requests_total'].value)
        self.assertEqual(0.0, s['requests_created'].value)
        self.assertEqual('agent-1', s['requests_total'].labels['stream_id'])

    def test_idempotent_replay(self):
        c = DeltaCollector()
        b = batch(sequence=0, updates=[counter(value=2.0)])
        self.assertTrue(c.apply(b))
        self.assertFalse(c.apply(b))
        self.assertFalse(c.apply(b))
        self.assertEqual(2.0, samples(c, 'requests')['requests_total'].value)

    def test_same_sequence_different_content_rejected(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, updates=[counter(value=2.0)]))
        with self.assertRaises(BatchConflictError):
            c.apply(batch(sequence=0, updates=[counter(value=3.0)]))
        self.assertEqual(2.0, samples(c, 'requests')['requests_total'].value)

    def test_stale_sequence_rejected(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, updates=[counter(value=1.0)]))
        c.apply(batch(sequence=1, start=10.0, end=20.0, updates=[counter(value=1.0)]))
        with self.assertRaises(OutOfOrderBatchError):
            c.apply(batch(sequence=0, updates=[counter(value=1.0)]))

    def test_sequence_gap_rejected(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, updates=[counter(value=1.0)]))
        with self.assertRaises(OutOfOrderBatchError):
            c.apply(batch(sequence=2, start=10.0, end=20.0,
                          updates=[counter(value=1.0)]))
        # The gap batch must not have been committed.
        self.assertEqual(1.0, samples(c, 'requests')['requests_total'].value)

    def test_interval_overlap_rejected(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, start=0.0, end=10.0, updates=[counter()]))
        with self.assertRaises(IntervalOverlapError):
            c.apply(batch(sequence=1, start=5.0, end=15.0, updates=[counter()]))

    def test_interval_gap_rejected(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, start=0.0, end=10.0, updates=[counter()]))
        with self.assertRaises(IntervalGapError):
            c.apply(batch(sequence=1, start=11.0, end=20.0, updates=[counter()]))

    def test_invalid_shapes_rejected(self):
        c = DeltaCollector()
        with self.assertRaises(InvalidDeltaError):
            c.apply(batch(start=10.0, end=10.0, updates=[counter()]))
        with self.assertRaises(InvalidDeltaError):
            c.apply(batch(updates=[counter(value=-1.0)]))
        with self.assertRaises(InvalidDeltaError):
            c.apply(batch(updates=[counter(labels={'stream_id': 'x'})]))


class TestDeltaCollectorGenerations(unittest.TestCase):
    def test_generation_change_resets_and_updates_created(self):
        c = DeltaCollector()
        c.apply(batch(generation=1, sequence=0, start=0.0, end=10.0,
                      updates=[counter(value=7.0)]))
        c.apply(batch(generation=2, sequence=0, start=100.0, end=110.0,
                      updates=[counter(value=2.0)]))
        s = samples(c, 'requests')
        self.assertEqual(2.0, s['requests_total'].value)
        self.assertEqual(100.0, s['requests_created'].value)

    def test_late_old_generation_rejected(self):
        c = DeltaCollector()
        c.apply(batch(generation=1, sequence=0, start=0.0, end=10.0,
                      updates=[counter(value=7.0)]))
        c.apply(batch(generation=2, sequence=0, start=100.0, end=110.0,
                      updates=[counter(value=2.0)]))
        with self.assertRaises(StaleGenerationError):
            c.apply(batch(generation=1, sequence=1, start=10.0, end=20.0,
                          updates=[counter(value=5.0)]))
        self.assertEqual(2.0, samples(c, 'requests')['requests_total'].value)

    def test_collector_restart_bootstraps_from_next_batch(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, updates=[counter(value=4.0)]))
        c.apply(batch(sequence=1, start=10.0, end=20.0, updates=[counter(value=4.0)]))
        # Simulated collector restart: state is lost, the agent keeps its
        # sequence going. The first batch seen bootstraps the stream.
        c = DeltaCollector()
        self.assertTrue(c.apply(batch(sequence=2, start=20.0, end=30.0,
                                      updates=[counter(value=4.0)])))
        s = samples(c, 'requests')
        self.assertEqual(4.0, s['requests_total'].value)
        self.assertEqual(20.0, s['requests_created'].value)
        # And the replayed restart batch is idempotent too.
        self.assertFalse(c.apply(batch(sequence=2, start=20.0, end=30.0,
                                       updates=[counter(value=4.0)])))


class TestDeltaCollectorHistograms(unittest.TestCase):
    def test_histogram_accumulates(self):
        c = DeltaCollector()
        c.apply(batch(sequence=0, updates=[histogram(count=3, sum=1.5,
                                                     buckets=((0.5, 2), (1.0, 1)))]))
        c.apply(batch(sequence=1, start=10.0, end=20.0,
                      updates=[histogram(count=2, sum=3.0,
                                         buckets=((0.5, 0), (1.0, 2)))]))
        s = samples(c, 'latency')
        by_le = {b.labels['le']: b.value for b in bucket_samples(c, 'latency')}
        self.assertEqual({'0.5': 2.0, '1.0': 5.0, '+Inf': 5.0}, by_le)
        self.assertEqual(5.0, s['latency_count'].value)
        self.assertEqual(4.5, s['latency_sum'].value)
        self.assertEqual(0.0, s['latency_created'].value)

    def test_bucket_total_mismatch_rejected(self):
        c = DeltaCollector()
        with self.assertRaises(HistogramValidationError):
            c.apply(batch(updates=[histogram(count=4, buckets=((0.5, 2), (1.0, 1)))]))
        self.assertEqual({}, samples(c, 'latency'))

    def test_bucket_layout_must_be_sorted_and_finite(self):
        c = DeltaCollector()
        with self.assertRaises(HistogramValidationError):
            c.apply(batch(updates=[histogram(count=2, buckets=((1.0, 1), (0.5, 1)))]))
        with self.assertRaises(HistogramValidationError):
            c.apply(batch(updates=[histogram(count=1, buckets=((float('inf'), 1),))]))

    def test_bucket_layout_change_requires_new_generation(self):
        c = DeltaCollector()
        c.apply(batch(generation=1, sequence=0,
                      updates=[histogram(buckets=((0.5, 2), (1.0, 1)))]))
        with self.assertRaises(BucketLayoutChangeError):
            c.apply(batch(generation=1, sequence=1, start=10.0, end=20.0,
                          updates=[histogram(count=1, sum=0.1,
                                             buckets=((0.1, 1), (1.0, 0)))]))
        # A new generation may establish a new layout.
        c.apply(batch(generation=2, sequence=0, start=100.0, end=110.0,
                      updates=[histogram(count=1, sum=0.1,
                                         buckets=((0.1, 1), (1.0, 0)))]))
        s = samples(c, 'latency')
        self.assertEqual(1.0, s['latency_count'].value)
        self.assertEqual(100.0, s['latency_created'].value)


class TestDeltaCollectorAtomicity(unittest.TestCase):
    def test_batch_commits_all_or_nothing(self):
        c = DeltaCollector()
        updates = [counter(value=2.0),
                   histogram(count=4, buckets=((0.5, 2), (1.0, 1)))]  # invalid
        with self.assertRaises(HistogramValidationError):
            c.apply(batch(updates=updates))
        # The valid counter update in the same batch must not be committed.
        self.assertEqual({}, samples(c, 'requests'))
        self.assertEqual({}, samples(c, 'latency'))
        # The failed batch was not recorded, so its sequence is still open.
        c.apply(batch(sequence=0, updates=[counter(value=2.0)]))
        self.assertEqual(2.0, samples(c, 'requests')['requests_total'].value)

    def test_collect_returns_consistent_snapshot(self):
        c = DeltaCollector()
        for i in range(50):
            c.apply(batch(sequence=i, start=i * 10.0, end=(i + 1) * 10.0,
                          updates=[counter(value=1.0),
                                   histogram(count=1, sum=0.5,
                                             buckets=((0.5, 0), (1.0, 1)))]))
        s = samples(c, 'latency')
        self.assertEqual(s['latency_count'].value,
                         s['latency_sum'].value * 2)
        self.assertEqual(50.0, samples(c, 'requests')['requests_total'].value)


class TestDeltaCollectorEviction(unittest.TestCase):
    def test_bounded_labels_evict_lru_and_report(self):
        c = DeltaCollector(max_series_per_stream=2)
        c.apply(batch(sequence=0, updates=[
            counter(labels={'path': '/a'}, value=1.0),
            counter(labels={'path': '/b'}, value=1.0),
        ]))
        # Touch /a so /b becomes the least recently used.
        c.apply(batch(sequence=1, start=10.0, end=20.0,
                      updates=[counter(labels={'path': '/a'}, value=1.0)]))
        c.apply(batch(sequence=2, start=20.0, end=30.0,
                      updates=[counter(labels={'path': '/c'}, value=1.0)]))
        self.assertEqual(1, c.evictions_total())
        self.assertEqual(1, c.evictions_total('agent-1'))
        (event,) = c.evictions
        self.assertEqual('agent-1', event.stream_id)
        self.assertEqual({'path': '/b'}, event.labels)
        self.assertEqual(2, event.sequence)
        # The self-metric is exposed.
        ev = samples(c, 'delta_collector_series_evictions')
        self.assertEqual(1.0, ev['delta_collector_series_evictions_total'].value)

    def test_evicted_series_restarts_without_faking_continuity(self):
        c = DeltaCollector(max_series_per_stream=1)
        c.apply(batch(sequence=0, start=0.0, end=10.0,
                      updates=[counter(labels={'path': '/a'}, value=5.0)]))
        c.apply(batch(sequence=1, start=10.0, end=20.0,
                      updates=[counter(labels={'path': '/b'}, value=1.0)]))
        # /a was evicted; its next delta starts a fresh series with a new
        # created timestamp instead of silently continuing from 5.0.
        c.apply(batch(sequence=2, start=20.0, end=30.0,
                      updates=[counter(labels={'path': '/a'}, value=2.0)]))
        s = samples(c, 'requests')
        self.assertEqual(2.0, s['requests_total'].value)
        self.assertEqual(20.0, s['requests_created'].value)
        self.assertEqual(2, c.evictions_total())


class TestDeltaCollectorRegistry(unittest.TestCase):
    def test_registers_and_exposes(self):
        registry = CollectorRegistry()
        c = DeltaCollector()
        registry.register(c)
        c.apply(batch(updates=[counter(value=3.0),
                               histogram(count=1, sum=0.5,
                                         buckets=((1.0, 1),))]))
        names = set()
        for metric in registry.collect():
            for s in metric.samples:
                names.add(s.name)
        self.assertIn('requests_total', names)
        self.assertIn('requests_created', names)
        self.assertIn('latency_bucket', names)
        self.assertIn('latency_count', names)
        self.assertIn('latency_sum', names)


if __name__ == '__main__':
    unittest.main()
