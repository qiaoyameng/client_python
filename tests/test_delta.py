import threading
import unittest

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.delta import (
    BatchConflictError, BucketLayoutChangedError, CounterDelta, DeltaBatch,
    DeltaCollector, DuplicateSeriesError, HistogramDelta,
    HistogramValidationError, IntervalGapError, IntervalOverlapError,
    InvalidDeltaError, SequenceGapError, StaleGenerationError,
    StaleSequenceError,
)


def counter(name='http_requests', path='/a', value=1.0):
    return CounterDelta(name, {'path': path}, value)


def histogram(buckets=((0.1, 2.0), (0.5, 1.0)), count=3.0, sum_=0.7, path='/a'):
    return HistogramDelta('request_seconds', {'path': path}, buckets, count, sum_)


def batch(seq, start, end, series, stream='agent-1', generation=1):
    return DeltaBatch(stream, generation, seq, start, end, tuple(series))


def fixed_script():
    """A fixed, deterministic sequence of batches used by replay/restart tests."""
    return [
        batch(1, 1000.0, 1005.0, [
            counter(path='/a', value=5.0),
            counter(path='/b', value=3.0),
            histogram(),
        ]),
        batch(2, 1005.0, 1010.0, [
            counter(path='/a', value=7.0),
            histogram(buckets=((0.1, 1.0), (0.5, 2.0)), sum_=1.1),
        ]),
        # The agent restarts: new generation, sequence and intervals restart.
        batch(1, 2000.0, 2005.0, [
            counter(path='/a', value=4.0),
            histogram(buckets=((0.1, 0.0), (0.5, 2.0)), count=2.0, sum_=0.9),
        ], generation=2),
    ]


def sample_map(collector):
    out = {}
    for metric in collector.collect():
        for sample in metric.samples:
            out[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return out


def counter_value(collector, path='/a'):
    return sample_map(collector).get(
        ('http_requests_total', (('path', path),)))


def created_value(collector, path='/a'):
    return sample_map(collector).get(
        ('http_requests_created', (('path', path),)))


class TestDeltaCollector(unittest.TestCase):
    def test_counter_accumulates_across_batches(self):
        collector = DeltaCollector()
        self.assertEqual('committed', collector.apply_batch(fixed_script()[0]))
        self.assertEqual('committed', collector.apply_batch(fixed_script()[1]))
        self.assertEqual(12.0, counter_value(collector, '/a'))
        self.assertEqual(3.0, counter_value(collector, '/b'))
        self.assertEqual(1000.0, created_value(collector, '/a'))

    def test_identical_replay_is_idempotent(self):
        collector = DeltaCollector()
        first = fixed_script()[0]
        collector.apply_batch(first)
        self.assertEqual('duplicate', collector.apply_batch(first))
        # Reordered series and label dicts are still the same content.
        reordered = DeltaBatch('agent-1', 1, 1, 1000.0, 1005.0, (
            HistogramDelta('request_seconds', {'path': '/a'},
                           ((0.1, 2.0), (0.5, 1.0)), 3.0, 0.7),
            CounterDelta('http_requests', {'path': '/b'}, 3.0),
            CounterDelta('http_requests', {'path': '/a'}, 5.0),
        ))
        self.assertEqual('duplicate', collector.apply_batch(reordered))
        self.assertEqual(5.0, counter_value(collector, '/a'))

    def test_same_sequence_different_content_rejected(self):
        collector = DeltaCollector()
        collector.apply_batch(fixed_script()[0])
        conflicting = batch(1, 1000.0, 1005.0, [counter(path='/a', value=99.0)])
        with self.assertRaises(BatchConflictError):
            collector.apply_batch(conflicting)
        self.assertEqual(5.0, counter_value(collector, '/a'))

    def test_stale_sequence_rejected(self):
        collector = DeltaCollector()
        script = fixed_script()
        collector.apply_batch(script[0])
        collector.apply_batch(script[1])
        older = batch(1, 1000.0, 1005.0, [counter(value=1.0)])
        # Not the most recent batch, so it cannot even be verified: reject.
        with self.assertRaises(StaleSequenceError):
            collector.apply_batch(older)

    def test_sequence_gap_rejected_then_filled(self):
        collector = DeltaCollector()
        collector.apply_batch(batch(1, 1000.0, 1005.0, [counter(value=1.0)]))
        with self.assertRaises(SequenceGapError):
            collector.apply_batch(batch(3, 1010.0, 1015.0, [counter(value=3.0)]))
        collector.apply_batch(batch(2, 1005.0, 1010.0, [counter(value=2.0)]))
        collector.apply_batch(batch(3, 1010.0, 1015.0, [counter(value=3.0)]))
        self.assertEqual(6.0, counter_value(collector))

    def test_interval_overlap_rejected(self):
        collector = DeltaCollector()
        collector.apply_batch(batch(1, 1000.0, 1005.0, [counter()]))
        with self.assertRaises(IntervalOverlapError):
            collector.apply_batch(batch(2, 1004.0, 1010.0, [counter()]))
        self.assertEqual(1.0, counter_value(collector))

    def test_interval_gap_rejected(self):
        collector = DeltaCollector()
        collector.apply_batch(batch(1, 1000.0, 1005.0, [counter()]))
        with self.assertRaises(IntervalGapError):
            collector.apply_batch(batch(2, 1006.0, 1010.0, [counter()]))
        self.assertEqual(1.0, counter_value(collector))

    def test_generation_change_resets_and_rejects_stale(self):
        collector = DeltaCollector()
        script = fixed_script()
        collector.apply_batch(script[0])
        collector.apply_batch(script[1])
        self.assertEqual(12.0, counter_value(collector, '/a'))
        # Agent restarts with a new generation: cumulative state is rebuilt.
        collector.apply_batch(script[2])
        self.assertEqual(4.0, counter_value(collector, '/a'))
        self.assertEqual(2000.0, created_value(collector, '/a'))
        # Series that only existed in the old generation are gone.
        self.assertIsNone(counter_value(collector, '/b'))
        # Late data from the old generation must not touch the new stream.
        late = batch(3, 1010.0, 1015.0, [counter(value=100.0)], generation=1)
        with self.assertRaises(StaleGenerationError):
            collector.apply_batch(late)
        late_replay = batch(2, 1005.0, 1010.0, [counter(value=7.0)], generation=1)
        with self.assertRaises(StaleGenerationError):
            collector.apply_batch(late_replay)
        self.assertEqual(4.0, counter_value(collector, '/a'))

    def test_histogram_accumulates(self):
        collector = DeltaCollector()
        collector.apply_batch(fixed_script()[0])
        collector.apply_batch(fixed_script()[1])
        samples = sample_map(collector)
        labels = (('path', '/a'),)
        self.assertEqual(3.0, samples[('request_seconds_bucket', (('le', '0.1'),) + labels)])
        self.assertEqual(6.0, samples[('request_seconds_bucket', (('le', '0.5'),) + labels)])
        self.assertEqual(6.0, samples[('request_seconds_bucket', (('le', '+Inf'),) + labels)])
        self.assertEqual(6.0, samples[('request_seconds_count', labels)])
        self.assertAlmostEqual(1.8, samples[('request_seconds_sum', labels)])
        self.assertEqual(1000.0, samples[('request_seconds_created', labels)])

    def test_histogram_bucket_total_mismatch_rejected(self):
        collector = DeltaCollector()
        bad = batch(1, 1000.0, 1005.0, [
            histogram(buckets=((0.1, 2.0), (0.5, 1.0)), count=4.0),
        ])
        with self.assertRaises(HistogramValidationError):
            collector.apply_batch(bad)

    def test_histogram_unordered_or_negative_buckets_rejected(self):
        collector = DeltaCollector()
        with self.assertRaises(HistogramValidationError):
            collector.apply_batch(batch(1, 1000.0, 1005.0, [
                histogram(buckets=((0.5, 1.0), (0.1, 2.0))),
            ]))
        with self.assertRaises(HistogramValidationError):
            collector.apply_batch(batch(1, 1000.0, 1005.0, [
                histogram(buckets=((0.1, -1.0), (0.5, 4.0))),
            ]))

    def test_bucket_layout_change_requires_new_generation(self):
        collector = DeltaCollector()
        collector.apply_batch(batch(1, 1000.0, 1005.0, [histogram()]))
        changed = batch(2, 1005.0, 1010.0, [
            histogram(buckets=((0.2, 1.0), (1.0, 2.0))),
        ])
        with self.assertRaises(BucketLayoutChangedError):
            collector.apply_batch(changed)
        # An explicit new generation may introduce a new layout.
        collector.apply_batch(batch(1, 2000.0, 2005.0, [
            histogram(buckets=((0.2, 1.0), (1.0, 2.0))),
        ], generation=2))
        samples = sample_map(collector)
        labels = (('path', '/a'),)
        self.assertEqual(1.0, samples[('request_seconds_bucket', (('le', '0.2'),) + labels)])
        self.assertNotIn(('request_seconds_bucket', (('le', '0.1'),) + labels), samples)

    def test_batch_is_validated_and_committed_atomically(self):
        collector = DeltaCollector()
        mixed = batch(1, 1000.0, 1005.0, [
            counter(path='/a', value=5.0),
            histogram(count=99.0),  # inconsistent: buckets sum to 3
        ])
        with self.assertRaises(HistogramValidationError):
            collector.apply_batch(mixed)
        # Nothing from the failed batch is visible...
        self.assertIsNone(counter_value(collector, '/a'))
        # ...and the sequence number was not consumed.
        collector.apply_batch(batch(1, 1000.0, 1005.0, [counter(path='/a', value=5.0)]))
        self.assertEqual(5.0, counter_value(collector, '/a'))

    def test_duplicate_series_within_batch_rejected(self):
        collector = DeltaCollector()
        with self.assertRaises(DuplicateSeriesError):
            collector.apply_batch(batch(1, 1000.0, 1005.0, [
                counter(value=1.0), counter(value=2.0),
            ]))

    def test_invalid_deltas_rejected(self):
        collector = DeltaCollector()
        with self.assertRaises(InvalidDeltaError):
            collector.apply_batch(batch(1, 1000.0, 1005.0, [counter(value=-1.0)]))
        with self.assertRaises(InvalidDeltaError):
            collector.apply_batch(batch(1, 1005.0, 1005.0, [counter()]))
        with self.assertRaises(InvalidDeltaError):
            collector.apply_batch(batch(1, 1000.0, 1005.0, [
                CounterDelta('http_requests', {'path': '/a'}, float('nan')),
            ]))

    def test_metric_name_type_and_labels_must_be_consistent(self):
        collector = DeltaCollector()
        collector.apply_batch(batch(1, 1000.0, 1005.0, [counter()]))
        with self.assertRaises(InvalidDeltaError):
            collector.apply_batch(batch(2, 1005.0, 1010.0, [
                CounterDelta('http_requests', {'method': 'get'}, 1.0),
            ]))
        with self.assertRaises(InvalidDeltaError):
            collector.apply_batch(batch(2, 1005.0, 1010.0, [
                HistogramDelta('http_requests', {'path': '/a'},
                               ((0.1, 1.0),), 1.0, 0.1),
            ]))

    def test_eviction_is_bounded_and_reported(self):
        collector = DeltaCollector(max_series=2)
        collector.apply_batch(batch(1, 1000.0, 1005.0, [counter(path='/a', value=1.0)]))
        collector.apply_batch(batch(2, 1005.0, 1010.0, [counter(path='/b', value=2.0)]))
        collector.apply_batch(batch(3, 1010.0, 1015.0, [counter(path='/c', value=4.0)]))
        self.assertEqual(1, collector.evictions_total)
        evictions = collector.recent_evictions()
        self.assertEqual(1, len(evictions))
        self.assertEqual('agent-1', evictions[0].stream)
        self.assertEqual('http_requests', evictions[0].name)
        self.assertEqual((('path', '/a'),), evictions[0].labels)
        # The evicted series is really gone.
        self.assertIsNone(counter_value(collector, '/a'))
        # A reappearing series restarts honestly: fresh created timestamp and
        # a cumulative value that does not pretend continuity.
        collector.apply_batch(batch(4, 1015.0, 1020.0, [counter(path='/a', value=8.0)]))
        self.assertEqual(8.0, counter_value(collector, '/a'))
        self.assertEqual(1015.0, created_value(collector, '/a'))
        # Eviction is also visible in the exported metrics.
        samples = sample_map(collector)
        self.assertEqual(2.0, samples[('delta_collector_evictions_total', ())])
        self.assertEqual(2.0, samples[('delta_collector_series', ())])

    def test_replay_of_fixed_script_is_idempotent(self):
        script = fixed_script()
        collector = DeltaCollector()
        for b in script:
            collector.apply_batch(b)
        snapshot = sample_map(collector)
        # Replaying the whole script changes nothing: old generations are
        # rejected, the current generation's last batch is a duplicate.
        results = []
        for b in script:
            try:
                results.append(collector.apply_batch(b))
            except StaleGenerationError:
                results.append('stale')
        self.assertEqual(['stale', 'stale', 'duplicate'], results)
        self.assertEqual(snapshot, sample_map(collector))

    def test_restart_replay_of_fixed_script_converges(self):
        script = fixed_script()
        first = DeltaCollector()
        for b in script:
            first.apply_batch(b)
        # A fresh collector (e.g. after a restart of this process) that
        # receives the same fixed sequence converges to the same state.
        second = DeltaCollector()
        for b in script:
            second.apply_batch(b)
        self.assertEqual(list(first.collect()), list(second.collect()))

    def test_collect_observes_pre_or_post_commit_snapshots(self):
        collector = DeltaCollector()
        batches = [
            batch(seq, 1000.0 + 5 * (seq - 1), 1000.0 + 5 * seq,
                  [counter(value=1.0)])
            for seq in range(1, 51)
        ]
        prefix_sums = {float(i) for i in range(0, 51)}
        errors = []

        def produce():
            for b in batches:
                collector.apply_batch(b)

        def consume():
            previous = 0.0
            for _ in range(200):
                value = counter_value(collector)
                if value is None:
                    continue
                if value not in prefix_sums or value < previous:
                    errors.append(value)
                previous = value

        producer = threading.Thread(target=produce)
        consumer = threading.Thread(target=consume)
        consumer.start()
        producer.start()
        producer.join()
        consumer.join()
        self.assertEqual([], errors)
        self.assertEqual(50.0, counter_value(collector))

    def test_registered_collector_exposition(self):
        registry = CollectorRegistry()
        collector = DeltaCollector()
        registry.register(collector)
        for b in fixed_script():
            collector.apply_batch(b)
        output = generate_latest(registry).decode()
        self.assertIn('http_requests_total{path="/a"} 4.0', output)
        self.assertIn('http_requests_created{path="/a"} 2000.0', output)
        self.assertIn('request_seconds_bucket{le="0.5",path="/a"} 2.0', output)
        self.assertIn('delta_collector_evictions_total 0.0', output)


if __name__ == '__main__':
    unittest.main()
