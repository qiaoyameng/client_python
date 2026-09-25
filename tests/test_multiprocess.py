import glob
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import warnings

from prometheus_client import mmap_dict, values
from prometheus_client.core import (
    CollectorRegistry, Counter, Gauge, Histogram, Sample, Summary,
)
from prometheus_client.multiprocess import (
    mark_process_dead, MultiProcessCollector,
)
from prometheus_client.values import (
    get_value_class, MultiProcessValue, MutexValue,
)


class TestMultiProcessDeprecation(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        os.environ.pop('prometheus_multiproc_dir', None)
        os.environ.pop('PROMETHEUS_MULTIPROC_DIR', None)
        values.close_all_multiprocess_files()
        values.ValueClass = MutexValue
        shutil.rmtree(self.tempdir)

    def test_deprecation_warning(self):
        os.environ['prometheus_multiproc_dir'] = self.tempdir
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            values.ValueClass = get_value_class()
            registry = CollectorRegistry()
            collector = MultiProcessCollector(registry)
            Counter('c', 'help', registry=None)

            assert os.environ['PROMETHEUS_MULTIPROC_DIR'] == self.tempdir
            if os.name != 'nt':
                assert len(w) == 1
                assert issubclass(w[-1].category, DeprecationWarning)
                assert "PROMETHEUS_MULTIPROC_DIR" in str(w[-1].message)
            else:
                assert len(w) == 0

    def test_mark_process_dead_respects_lowercase(self):
        os.environ['prometheus_multiproc_dir'] = self.tempdir
        # Just test that this does not raise with a lowercase env var. The
        # logic is tested elsewhere.
        mark_process_dead(123)


class TestMultiProcess(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = self.tempdir
        values.ValueClass = MultiProcessValue(lambda: 123)
        self.registry = CollectorRegistry(support_collectors_without_names=True)
        self.collector = MultiProcessCollector(self.registry)

    @property
    def _value_class(self):
        return

    def tearDown(self):
        del os.environ['PROMETHEUS_MULTIPROC_DIR']
        values.close_all_multiprocess_files()
        values.ValueClass = MutexValue
        shutil.rmtree(self.tempdir)

    def test_counter_adds(self):
        c1 = Counter('c', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        c2 = Counter('c', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('c_total'))
        c1.inc(1)
        c2.inc(2)
        self.assertEqual(3, self.registry.get_sample_value('c_total'))

    def test_summary_adds(self):
        s1 = Summary('s', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        s2 = Summary('s', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('s_count'))
        self.assertEqual(0, self.registry.get_sample_value('s_sum'))
        s1.observe(1)
        s2.observe(2)
        self.assertEqual(2, self.registry.get_sample_value('s_count'))
        self.assertEqual(3, self.registry.get_sample_value('s_sum'))

    def test_histogram_adds(self):
        h1 = Histogram('h', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        h2 = Histogram('h', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('h_count'))
        self.assertEqual(0, self.registry.get_sample_value('h_sum'))
        self.assertEqual(0, self.registry.get_sample_value('h_bucket', {'le': '5.0'}))
        h1.observe(1)
        h2.observe(2)
        self.assertEqual(2, self.registry.get_sample_value('h_count'))
        self.assertEqual(3, self.registry.get_sample_value('h_sum'))
        self.assertEqual(2, self.registry.get_sample_value('h_bucket', {'le': '5.0'}))

    def test_gauge_all(self):
        g1 = Gauge('g', 'help', registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '456'}))
        g1.set(1)
        g2.set(2)
        mark_process_dead(123)
        self.assertEqual(1, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g', {'pid': '456'}))

    def test_gauge_liveall(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='liveall')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='liveall')
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(0, self.registry.get_sample_value('g', {'pid': '456'}))
        g1.set(1)
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g', {'pid': '456'}))
        values.close_all_multiprocess_files()
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(None, self.registry.get_sample_value('g', {'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g', {'pid': '456'}))

    def test_gauge_min(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='min')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='min')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('g'))

    def test_gauge_livemin(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livemin')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livemin')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('g'))
        values.close_all_multiprocess_files()
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_gauge_max(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='max')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='max')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_gauge_livemax(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livemax')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livemax')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(2)
        g2.set(1)
        self.assertEqual(2, self.registry.get_sample_value('g'))
        values.close_all_multiprocess_files()
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(1, self.registry.get_sample_value('g'))

    def test_gauge_sum(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='sum')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='sum')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(3, self.registry.get_sample_value('g'))
        values.close_all_multiprocess_files()
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(3, self.registry.get_sample_value('g'))

    def test_gauge_livesum(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livesum')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livesum')
        self.assertEqual(0, self.registry.get_sample_value('g'))
        g1.set(1)
        g2.set(2)
        self.assertEqual(3, self.registry.get_sample_value('g'))
        values.close_all_multiprocess_files()
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_gauge_mostrecent(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='mostrecent')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='mostrecent')
        g2.set(2)
        g1.set(1)
        self.assertEqual(1, self.registry.get_sample_value('g'))
        values.close_all_multiprocess_files()
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(1, self.registry.get_sample_value('g'))

    def test_gauge_livemostrecent(self):
        g1 = Gauge('g', 'help', registry=None, multiprocess_mode='livemostrecent')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', registry=None, multiprocess_mode='livemostrecent')
        g2.set(2)
        g1.set(1)
        self.assertEqual(1, self.registry.get_sample_value('g'))
        values.close_all_multiprocess_files()
        mark_process_dead(123, os.environ['PROMETHEUS_MULTIPROC_DIR'])
        self.assertEqual(2, self.registry.get_sample_value('g'))

    def test_namespace_subsystem(self):
        c1 = Counter('c', 'help', registry=None, namespace='ns', subsystem='ss')
        c1.inc(1)
        self.assertEqual(1, self.registry.get_sample_value('ns_ss_c_total'))

    def test_counter_across_forks(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        c1 = Counter('c', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('c_total'))
        c1.inc(1)
        c1.inc(1)
        pid = 1
        c1.inc(1)
        self.assertEqual(3, self.registry.get_sample_value('c_total'))
        self.assertEqual(1, c1._value.get())

    def test_initialization_detects_pid_change(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)

        # can not inspect the files cache directly, as it's a closure, so we
        # check for the actual files themselves
        def files():
            fs = os.listdir(os.environ['PROMETHEUS_MULTIPROC_DIR'])
            fs.sort()
            return fs

        c1 = Counter('c1', 'c1', registry=None)
        self.assertEqual(files(), ['counter_0.db'])
        c2 = Counter('c2', 'c2', registry=None)
        self.assertEqual(files(), ['counter_0.db'])
        pid = 1
        c3 = Counter('c3', 'c3', registry=None)
        self.assertEqual(files(), ['counter_0.db', 'counter_1.db'])

    def test_collect(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        c = Counter('c', 'help', labelnames=labels.keys(), registry=None)
        g = Gauge('g', 'help', labelnames=labels.keys(), registry=None)
        h = Histogram('h', 'help', labelnames=labels.keys(), registry=None)

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(1)

        pid = 1

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(5)

        metrics = {m.name: m for m in self.collector.collect()}

        self.assertEqual(
            metrics['c'].samples, [Sample('c_total', labels, 2.0)]
        )
        metrics['g'].samples.sort(key=lambda x: x[1]['pid'])
        self.assertEqual(metrics['g'].samples, [
            Sample('g', add_label('pid', '0'), 1.0),
            Sample('g', add_label('pid', '1'), 1.0),
        ])

        expected_histogram = [
            Sample('h_sum', labels, 6.0),
            Sample('h_bucket', add_label('le', '0.005'), 0.0),
            Sample('h_bucket', add_label('le', '0.01'), 0.0),
            Sample('h_bucket', add_label('le', '0.025'), 0.0),
            Sample('h_bucket', add_label('le', '0.05'), 0.0),
            Sample('h_bucket', add_label('le', '0.075'), 0.0),
            Sample('h_bucket', add_label('le', '0.1'), 0.0),
            Sample('h_bucket', add_label('le', '0.25'), 0.0),
            Sample('h_bucket', add_label('le', '0.5'), 0.0),
            Sample('h_bucket', add_label('le', '0.75'), 0.0),
            Sample('h_bucket', add_label('le', '1.0'), 1.0),
            Sample('h_bucket', add_label('le', '2.5'), 1.0),
            Sample('h_bucket', add_label('le', '5.0'), 2.0),
            Sample('h_bucket', add_label('le', '7.5'), 2.0),
            Sample('h_bucket', add_label('le', '10.0'), 2.0),
            Sample('h_bucket', add_label('le', '+Inf'), 2.0),
            Sample('h_count', labels, 2.0),
        ]

        self.assertEqual(metrics['h'].samples, expected_histogram)

    def test_collect_histogram_ordering(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        h = Histogram('h', 'help', labelnames=['view'], registry=None)

        h.labels(view='view1').observe(1)

        pid = 1

        h.labels(view='view1').observe(5)
        h.labels(view='view2').observe(1)

        metrics = {m.name: m for m in self.collector.collect()}

        expected_histogram = [
            Sample('h_sum', {'view': 'view1'}, 6.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.005'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.01'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.025'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.05'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.075'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.1'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.25'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.5'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '0.75'}, 0.0),
            Sample('h_bucket', {'view': 'view1', 'le': '1.0'}, 1.0),
            Sample('h_bucket', {'view': 'view1', 'le': '2.5'}, 1.0),
            Sample('h_bucket', {'view': 'view1', 'le': '5.0'}, 2.0),
            Sample('h_bucket', {'view': 'view1', 'le': '7.5'}, 2.0),
            Sample('h_bucket', {'view': 'view1', 'le': '10.0'}, 2.0),
            Sample('h_bucket', {'view': 'view1', 'le': '+Inf'}, 2.0),
            Sample('h_count', {'view': 'view1'}, 2.0),
            Sample('h_sum', {'view': 'view2'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.005'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.01'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.025'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.05'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.075'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.1'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.25'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.5'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '0.75'}, 0.0),
            Sample('h_bucket', {'view': 'view2', 'le': '1.0'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '2.5'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '5.0'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '7.5'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '10.0'}, 1.0),
            Sample('h_bucket', {'view': 'view2', 'le': '+Inf'}, 1.0),
            Sample('h_count', {'view': 'view2'}, 1.0),
        ]

        self.assertEqual(metrics['h'].samples, expected_histogram)

    def test_restrict(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        c = Counter('c', 'help', labelnames=labels.keys(), registry=None)
        g = Gauge('g', 'help', labelnames=labels.keys(), registry=None)

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)

        pid = 1

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)

        metrics = {m.name: m for m in self.registry.restricted_registry(['c_total']).collect()}

        self.assertEqual(metrics.keys(), {'c'})

        self.assertEqual(
            metrics['c'].samples, [Sample('c_total', labels, 2.0)]
        )

    def test_collect_preserves_help(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        c = Counter('c', 'c help', labelnames=labels.keys(), registry=None)
        g = Gauge('g', 'g help', labelnames=labels.keys(), registry=None)
        h = Histogram('h', 'h help', labelnames=labels.keys(), registry=None)

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(1)

        pid = 1

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        h.labels(**labels).observe(5)

        metrics = {m.name: m for m in self.collector.collect()}

        self.assertEqual(metrics['c'].documentation, 'c help')
        self.assertEqual(metrics['g'].documentation, 'g help')
        self.assertEqual(metrics['h'].documentation, 'h help')

    def test_merge_no_accumulate(self):
        pid = 0
        values.ValueClass = MultiProcessValue(lambda: pid)
        labels = {i: i for i in 'abcd'}

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        h = Histogram('h', 'help', labelnames=labels.keys(), registry=None)
        h.labels(**labels).observe(1)
        pid = 1
        h.labels(**labels).observe(5)

        path = os.path.join(os.environ['PROMETHEUS_MULTIPROC_DIR'], '*.db')
        files = glob.glob(path)
        metrics = {
            m.name: m for m in self.collector.merge(files, accumulate=False)
        }

        expected_histogram = [
            Sample('h_sum', labels, 6.0),
            Sample('h_bucket', add_label('le', '0.005'), 0.0),
            Sample('h_bucket', add_label('le', '0.01'), 0.0),
            Sample('h_bucket', add_label('le', '0.025'), 0.0),
            Sample('h_bucket', add_label('le', '0.05'), 0.0),
            Sample('h_bucket', add_label('le', '0.075'), 0.0),
            Sample('h_bucket', add_label('le', '0.1'), 0.0),
            Sample('h_bucket', add_label('le', '0.25'), 0.0),
            Sample('h_bucket', add_label('le', '0.5'), 0.0),
            Sample('h_bucket', add_label('le', '0.75'), 0.0),
            Sample('h_bucket', add_label('le', '1.0'), 1.0),
            Sample('h_bucket', add_label('le', '2.5'), 0.0),
            Sample('h_bucket', add_label('le', '5.0'), 1.0),
            Sample('h_bucket', add_label('le', '7.5'), 0.0),
            Sample('h_bucket', add_label('le', '10.0'), 0.0),
            Sample('h_bucket', add_label('le', '+Inf'), 0.0),
        ]

        self.assertEqual(metrics['h'].samples, expected_histogram)

    def test_missing_gauge_file_during_merge(self):
        # These files don't exist, just like if mark_process_dead(9999999) had been
        # called during self.collector.collect(), after the glob found it
        # but before the merge actually happened.
        # This should not raise and return no metrics
        self.assertFalse(self.collector.merge([
            os.path.join(self.tempdir, 'gauge_liveall_9999999.db'),
            os.path.join(self.tempdir, 'gauge_livesum_9999999.db'),
        ]))

    def test_remove_counter_across_processes(self):
        c1 = Counter('c', 'help', labelnames=['l'], registry=None)
        c1_child = c1.labels('x')
        c1_other = c1.labels('y')
        values.ValueClass = MultiProcessValue(lambda: 456)
        c2 = Counter('c', 'help', labelnames=['l'], registry=None)
        stale_child = c2.labels('x')
        c1_child.inc(5)
        stale_child.inc(3)
        c1_other.inc(7)
        self.assertEqual(8, self.registry.get_sample_value('c_total', {'l': 'x'}))
        c1.remove('x')
        # The series is gone from every process file, other series are kept.
        self.assertIsNone(self.registry.get_sample_value('c_total', {'l': 'x'}))
        self.assertEqual(7, self.registry.get_sample_value('c_total', {'l': 'y'}))
        # A stale handle in another process only contributes its delta after
        # the deletion, it must not resurrect the old value.
        stale_child.inc(2)
        self.assertEqual(2, self.registry.get_sample_value('c_total', {'l': 'x'}))

    def test_remove_and_recreate_series(self):
        c = Counter('c', 'help', labelnames=['l'], registry=None)
        c.labels('x').inc(5)
        c.remove('x')
        self.assertIsNone(self.registry.get_sample_value('c_total', {'l': 'x'}))
        # Recreating a series with the same labels starts from scratch.
        c.labels('x').inc(4)
        self.assertEqual(4, self.registry.get_sample_value('c_total', {'l': 'x'}))

    def test_remove_is_idempotent(self):
        c = Counter('c', 'help', labelnames=['l'], registry=None)
        c.labels('x').inc(5)
        c.remove('x')
        c.remove('x')
        # Removing a series that never existed is a no-op.
        c.remove('never_existed')
        self.assertIsNone(self.registry.get_sample_value('c_total', {'l': 'x'}))

    def test_remove_by_labels_across_processes(self):
        c1 = Counter('c', 'help', labelnames=['tenant', 'path'], registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        c2 = Counter('c', 'help', labelnames=['tenant', 'path'], registry=None)
        c1.labels('t1', '/a').inc(1)
        c2.labels('t1', '/b').inc(2)
        c2.labels('t2', '/a').inc(4)
        c1.remove_by_labels({'tenant': 't1'})
        self.assertIsNone(self.registry.get_sample_value('c_total', {'tenant': 't1', 'path': '/a'}))
        self.assertIsNone(self.registry.get_sample_value('c_total', {'tenant': 't1', 'path': '/b'}))
        self.assertEqual(4, self.registry.get_sample_value('c_total', {'tenant': 't2', 'path': '/a'}))

    def test_clear_across_processes(self):
        c1 = Counter('c', 'help', labelnames=['l'], registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        c2 = Counter('c', 'help', labelnames=['l'], registry=None)
        c1.labels('x').inc(1)
        c2.labels('y').inc(2)
        c2.clear()
        self.assertIsNone(self.registry.get_sample_value('c_total', {'l': 'x'}))
        self.assertIsNone(self.registry.get_sample_value('c_total', {'l': 'y'}))
        # The metric can be reused after a clear.
        c1.labels('z').inc(3)
        self.assertEqual(3, self.registry.get_sample_value('c_total', {'l': 'z'}))

    def test_remove_histogram_series(self):
        h1 = Histogram('h', 'help', labelnames=['l'], registry=None, buckets=[1, 5])
        h1_child = h1.labels('x')
        h1_other = h1.labels('y')
        values.ValueClass = MultiProcessValue(lambda: 456)
        h2 = Histogram('h', 'help', labelnames=['l'], registry=None, buckets=[1, 5])
        stale_child = h2.labels('x')
        h1_child.observe(0.5)
        stale_child.observe(3)
        h1_other.observe(2)
        self.assertEqual(2, self.registry.get_sample_value('h_count', {'l': 'x'}))
        h1.remove('x')
        # The whole series (buckets, sum and count) is gone.
        self.assertIsNone(self.registry.get_sample_value('h_count', {'l': 'x'}))
        self.assertIsNone(self.registry.get_sample_value('h_sum', {'l': 'x'}))
        self.assertIsNone(self.registry.get_sample_value('h_bucket', {'l': 'x', 'le': '1.0'}))
        self.assertEqual(1, self.registry.get_sample_value('h_count', {'l': 'y'}))
        # A stale handle only records observations made after the deletion.
        stale_child.observe(4)
        self.assertEqual(1, self.registry.get_sample_value('h_count', {'l': 'x'}))
        self.assertEqual(4, self.registry.get_sample_value('h_sum', {'l': 'x'}))
        self.assertEqual(1, self.registry.get_sample_value('h_bucket', {'l': 'x', 'le': '5.0'}))
        # Samples the stale handle did not touch stay deleted; recreating the
        # child via remove() + .labels() re-initializes the complete series.
        self.assertIsNone(self.registry.get_sample_value('h_bucket', {'l': 'x', 'le': '1.0'}))
        h2.remove('x')
        h2.labels('x')
        self.assertEqual(0, self.registry.get_sample_value('h_bucket', {'l': 'x', 'le': '1.0'}))
        self.assertEqual(0, self.registry.get_sample_value('h_count', {'l': 'x'}))

    def test_remove_gauge_modes(self):
        for mode in ('all', 'min', 'max', 'sum', 'mostrecent',
                     'liveall', 'livemin', 'livemax', 'livesum', 'livemostrecent'):
            g1 = Gauge('g_' + mode, 'help', labelnames=['l'], registry=None, multiprocess_mode=mode)
            g1_child = g1.labels('x')
            values.ValueClass = MultiProcessValue(lambda: 456)
            g2 = Gauge('g_' + mode, 'help', labelnames=['l'], registry=None, multiprocess_mode=mode)
            g1_child.set(1)
            g2.labels('x').set(2)
            g1.remove('x')
            self.assertFalse(
                any(s.labels.get('l') == 'x'
                    for m in self.collector.collect() if m.name == 'g_' + mode
                    for s in m.samples),
                mode)
            values.ValueClass = MultiProcessValue(lambda: 123)

    def test_remove_gauge_sum_stale_handle(self):
        g1 = Gauge('g', 'help', labelnames=['l'], registry=None, multiprocess_mode='sum')
        g1_child = g1.labels('x')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', labelnames=['l'], registry=None, multiprocess_mode='sum')
        stale_child = g2.labels('x')
        g1_child.inc(10)
        stale_child.inc(5)
        self.assertEqual(15, self.registry.get_sample_value('g', {'l': 'x'}))
        g1.remove('x')
        self.assertIsNone(self.registry.get_sample_value('g', {'l': 'x'}))
        # Only the delta after the deletion is visible.
        stale_child.inc(2)
        self.assertEqual(2, self.registry.get_sample_value('g', {'l': 'x'}))
        # An absolute set recreates the series with the new value.
        stale_child.set(42)
        self.assertEqual(42, self.registry.get_sample_value('g', {'l': 'x'}))

    def test_remove_gauge_mostrecent_stale_handle(self):
        g1 = Gauge('g', 'help', labelnames=['l'], registry=None, multiprocess_mode='mostrecent')
        g1_child = g1.labels('x')
        values.ValueClass = MultiProcessValue(lambda: 456)
        g2 = Gauge('g', 'help', labelnames=['l'], registry=None, multiprocess_mode='mostrecent')
        stale_child = g2.labels('x')
        g1_child.set(1)
        stale_child.set(2)
        g1.remove('x')
        self.assertIsNone(self.registry.get_sample_value('g', {'l': 'x'}))
        stale_child.set(3)
        self.assertEqual(3, self.registry.get_sample_value('g', {'l': 'x'}))

    def test_old_format_file_compatibility(self):
        # Files written by older versions have zero padding where the deletion
        # generation now lives. They must be readable, and deletable, as
        # generation 0 files.
        key = mmap_dict.mmap_key('c', 'c_total', ('l',), ('x',), 'help')
        encoded = key.encode('utf-8')
        padded = encoded + b' ' * (8 - (len(encoded) + 4) % 8)
        entry = struct.pack('i', len(encoded)) + padded + struct.pack('dd', 11.0, 0.0)
        filename = os.path.join(self.tempdir, 'counter_123.db')
        with open(filename, 'wb') as f:
            f.write(struct.pack('ii', 8 + len(entry), 0))
            f.write(entry)
            f.truncate(mmap_dict._INITIAL_MMAP_SIZE)
        self.assertEqual(11, self.registry.get_sample_value('c_total', {'l': 'x'}))
        c = Counter('c', 'help', labelnames=['l'], registry=None)
        c.remove('x')
        self.assertIsNone(self.registry.get_sample_value('c_total', {'l': 'x'}))
        # The file was upgraded in place: generation bumped, entry tombstoned.
        d = mmap_dict.MmapedDict(filename)
        self.assertEqual(1, d.read_generation())
        self.assertEqual([], list(d.read_all_values()))
        d.close()
        # New writes to the same series start fresh.
        c.labels('x').inc(3)
        self.assertEqual(3, self.registry.get_sample_value('c_total', {'l': 'x'}))
    
    def test_child_name_is_built_once_with_namespace_subsystem_unit(self):
        """
        Repro for #1035:
        In multiprocess mode, child metrics must NOT rebuild the full name
        (namespace/subsystem/unit) a second time. The exported family name should
        be built once, and Counter samples should use "<family>_total".
        """
        from prometheus_client import Counter

        class CustomCounter(Counter):
            def __init__(
                self, 
                name, 
                documentation, 
                labelnames=(),
                namespace="mydefaultnamespace", 
                subsystem="mydefaultsubsystem",
                unit="", 
                registry=None, 
                _labelvalues=None
            ):
                # Intentionally provide non-empty defaults to trigger the bug path.
                super().__init__(
                    name=name, 
                    documentation=documentation,
                    labelnames=labelnames, 
                    namespace=namespace,
                    subsystem=subsystem, 
                    unit=unit, 
                    registry=registry,
                    _labelvalues=_labelvalues)

        # Create a Counter with explicit namespace/subsystem/unit
        c = CustomCounter(
            name='m',
            documentation='help',
            labelnames=('status', 'method'),
            namespace='ns',
            subsystem='ss',
            unit='seconds',   # avoid '_total_total' confusion
            registry=None,    # not registered in local registry in multiprocess mode
        )

        # Create two labeled children
        c.labels(status='200', method='GET').inc()
        c.labels(status='404', method='POST').inc()

        # Collect from the multiprocess collector initialized in setUp()
        metrics = {m.name: m for m in self.collector.collect()}

        # Family name should be built once (no '_total' in family name)
        expected_family = 'ns_ss_m_seconds'
        self.assertIn(expected_family, metrics, f"missing family {expected_family}")

        # Counter samples must use '<family>_total'
        mf = metrics[expected_family]
        sample_names = {s.name for s in mf.samples}
        self.assertTrue(
            all(name == expected_family + '_total' for name in sample_names),
            f"unexpected sample names: {sample_names}"
        )

        # Ensure no double-built prefix sneaks in (the original bug)
        bad_prefix = 'mydefaultnamespace_mydefaultsubsystem_'
        all_names = {mf.name, *sample_names}
        self.assertTrue(
            all(not n.startswith(bad_prefix) for n in all_names),
            f"found double-built name(s): {[n for n in all_names if n.startswith(bad_prefix)]}"
        )

    def test_child_preserves_parent_context_for_subclasses(self):
        """
        Ensure child metrics preserve parent's namespace/subsystem/unit information
        so that subclasses can correctly use these parameters in their logic.
        """
        class ContextAwareCounter(Counter):
            def __init__(self,
                         name,
                         documentation,
                         labelnames=(),
                         namespace="",
                         subsystem="",
                         unit="",
                         **kwargs):
                self.context = {
                    'namespace': namespace,
                    'subsystem': subsystem,
                    'unit': unit
                }
                super().__init__(name, documentation,
                                 labelnames=labelnames,
                                 namespace=namespace,
                                 subsystem=subsystem,
                                 unit=unit,
                                 **kwargs)

        parent = ContextAwareCounter('m', 'help',
                                     labelnames=['status'],
                                     namespace='prod',
                                     subsystem='api',
                                     unit='seconds',
                                     registry=None)

        child = parent.labels(status='200')

        # Verify that child retains parent's context
        self.assertEqual(child.context['namespace'], 'prod')
        self.assertEqual(child.context['subsystem'], 'api')
        self.assertEqual(child.context['unit'], 'seconds')


class TestMmapedDict(unittest.TestCase):
    def setUp(self):
        fd, self.tempfile = tempfile.mkstemp()
        os.close(fd)
        self.d = mmap_dict.MmapedDict(self.tempfile)

    def test_process_restart(self):
        self.d.write_value('abc', 123.0, 987.0)
        self.d.close()
        self.d = mmap_dict.MmapedDict(self.tempfile)
        self.assertEqual((123, 987.0), self.d.read_value('abc'))
        self.assertEqual([('abc', 123.0, 987.0)], list(self.d.read_all_values()))

    def test_expansion(self):
        key = 'a' * mmap_dict._INITIAL_MMAP_SIZE
        self.d.write_value(key, 123.0, 987.0)
        self.assertEqual([(key, 123.0, 987.0)], list(self.d.read_all_values()))

    def test_multi_expansion(self):
        key = 'a' * mmap_dict._INITIAL_MMAP_SIZE * 4
        self.d.write_value('abc', 42.0, 987.0)
        self.d.write_value(key, 123.0, 876.0)
        self.d.write_value('def', 17.0, 765.0)
        self.assertEqual(
            [('abc', 42.0, 987.0), (key, 123.0, 876.0), ('def', 17.0, 765.0)],
            list(self.d.read_all_values()))

    def test_corruption_detected(self):
        self.d.write_value('abc', 42.0, 987.0)
        # corrupt the written data
        self.d._m[8:16] = b'somejunk'
        with self.assertRaises(RuntimeError):
            list(self.d.read_all_values())

    def test_delete_matching_tombstones(self):
        self.d.write_value('abc', 1.0, 0.0)
        self.d.write_value('def', 2.0, 0.0)
        self.assertEqual(1, self.d.delete_matching(lambda k: k == 'abc'))
        self.assertEqual([('def', 2.0, 0.0)], list(self.d.read_all_values()))
        # The deletion generation was bumped.
        self.assertEqual(1, self.d.read_generation())
        # A second deletion matching nothing neither changes data nor bumps
        # the generation, so other handles are not unnecessarily invalidated.
        self.assertEqual(0, self.d.delete_matching(lambda k: k == 'abc'))
        self.assertEqual(1, self.d.read_generation())
        # Positions of surviving entries still work.
        self.d.write_value('def', 5.0, 0.0)
        self.assertEqual([('def', 5.0, 0.0)], list(self.d.read_all_values()))

    def test_sync_picks_up_external_tombstone(self):
        self.d.write_value('abc', 100.0, 0.0)
        # Simulate another process deleting the entry with a second handle.
        other = mmap_dict.MmapedDict(self.tempfile)
        other.delete_matching(lambda k: k == 'abc')
        other.close()
        self.assertTrue(self.d.sync())
        self.assertNotIn('abc', self.d._positions)
        self.assertEqual(None, self.d.read_value_if_present('abc'))
        self.assertEqual([], list(self.d.read_all_values()))
        # Writing recreates the series from zero.
        self.d.write_value('abc', 1.0, 0.0)
        self.assertEqual([('abc', 1.0, 0.0)], list(self.d.read_all_values()))
        self.assertFalse(self.d.sync())

    def test_old_files_have_generation_zero(self):
        fd, path = tempfile.mkstemp()
        os.close(fd)
        try:
            with open(path, 'wb') as f:
                f.write(struct.pack('ii', 8, 0))
                f.truncate(mmap_dict._INITIAL_MMAP_SIZE)
            d = mmap_dict.MmapedDict(path)
            self.assertEqual(0, d.read_generation())
            d.close()
        finally:
            os.unlink(path)

    def tearDown(self):
        self.d.close()
        os.unlink(self.tempfile)


class TestUnsetEnv(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        fp, self.tmpfl = tempfile.mkstemp()
        os.close(fp)

    def test_unset_syncdir_env(self):
        self.assertRaises(
            ValueError, MultiProcessCollector, self.registry)

    def test_file_syncpath(self):
        registry = CollectorRegistry()
        self.assertRaises(
            ValueError, MultiProcessCollector, registry, self.tmpfl)

    def tearDown(self):
        os.remove(self.tmpfl)

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# A worker process that keeps a reference to a labelled child across a deletion
# performed by another process. It writes before and after the deletion.
_STALE_HANDLE_SCRIPT = r'''
import os
import time
from prometheus_client import Counter, Gauge, Histogram

sync_dir = os.environ['SYNC_DIR']
kind = os.environ['METRIC_KIND']


def wait_for(name):
    while not os.path.exists(os.path.join(sync_dir, name)):
        time.sleep(0.01)


if kind == 'counter':
    metric = Counter('m', 'help', ['tenant'], registry=None)
    child = metric.labels('t1')
    child.inc(10)
    wait_for('ready-ack')
    open(os.path.join(sync_dir, 'ready'), 'w').close()
    wait_for('deleted')
    child.inc(1)
elif kind == 'gauge':
    metric = Gauge('m', 'help', ['tenant'], registry=None, multiprocess_mode='livesum')
    child = metric.labels('t1')
    child.inc(10)
    wait_for('ready-ack')
    open(os.path.join(sync_dir, 'ready'), 'w').close()
    wait_for('deleted')
    child.inc(1)
elif kind == 'histogram':
    metric = Histogram('m', 'help', ['tenant'], registry=None, buckets=[1.0, 5.0])
    child = metric.labels('t1')
    child.observe(3)
    wait_for('ready-ack')
    open(os.path.join(sync_dir, 'ready'), 'w').close()
    wait_for('deleted')
    child.observe(4)

open(os.path.join(sync_dir, 'done'), 'w').close()
wait_for('exit')
'''

_REMOVE_SCRIPT = r'''
from prometheus_client import Counter, Gauge, Histogram

kind = __import__('os').environ['METRIC_KIND']
if kind == 'counter':
    Counter('m', 'help', ['tenant'], registry=None).remove('t1')
elif kind == 'gauge':
    Gauge('m', 'help', ['tenant'], registry=None, multiprocess_mode='livesum').remove('t1')
elif kind == 'histogram':
    Histogram('m', 'help', ['tenant'], registry=None, buckets=[1.0, 5.0]).remove('t1')
'''

_INC_SCRIPT = r'''
import os
from prometheus_client import Counter

c = Counter('c', 'help', ['tenant'], registry=None)
c.labels('t1').inc(float(os.environ['AMOUNT']))
'''

_REMOVE_COUNTER_SCRIPT = r'''
from prometheus_client import Counter

c = Counter('c', 'help', ['tenant'], registry=None)
c.remove('t1')
# Removing a series that never existed is a no-op.
c.remove('never_existed')
# Repeated removal of the same series is a no-op.
c.remove('t1')
'''


@unittest.skipIf(os.name == 'nt', 'POSIX mmap and process semantics required')
class TestMultiProcessSubprocesses(unittest.TestCase):
    """Tests with real OS processes: a deleter, a writer holding a stale handle,
    and a collector run as independent processes."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.sync_dir = os.path.join(self.tempdir, 'sync')
        os.makedirs(self.sync_dir)
        self.env = dict(
            os.environ,
            PROMETHEUS_MULTIPROC_DIR=self.tempdir,
            PYTHONPATH=_REPO_ROOT + os.pathsep + os.environ.get('PYTHONPATH', ''),
        )

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _run(self, script, extra_env=None, timeout=30):
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)
        result = subprocess.run(
            [sys.executable, '-c', script],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=timeout)
        if result.returncode != 0:
            raise AssertionError('subprocess failed:\n' + result.stderr.decode('utf-8'))

    def _start(self, script, extra_env=None):
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)
        return subprocess.Popen(
            [sys.executable, '-c', script],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def _wait_for(self, name, timeout=15):
        deadline = time.time() + timeout
        while not os.path.exists(os.path.join(self.sync_dir, name)):
            self.assertLess(time.time(), deadline, 'timed out waiting for %s' % name)
            time.sleep(0.02)

    def _touch(self, name):
        open(os.path.join(self.sync_dir, name), 'w').close()

    def _collect_value(self, name, labels=None):
        registry = CollectorRegistry()
        MultiProcessCollector(registry, path=self.tempdir)
        return registry.get_sample_value(name, labels)

    def _stale_handle_scenario(self, kind, sample_name, expected_after_delete):
        worker = self._start(_STALE_HANDLE_SCRIPT,
                             {'SYNC_DIR': self.sync_dir, 'METRIC_KIND': kind})
        try:
            self._touch('ready-ack')
            self._wait_for('ready')
            self.assertEqual(expected_after_delete[0], self._collect_value(sample_name, {'tenant': 't1'}))
            # A second, independent process performs the removal.
            self._run(_REMOVE_SCRIPT, {'METRIC_KIND': kind})
            self.assertIsNone(self._collect_value(sample_name, {'tenant': 't1'}))
            # The writer still holds the old child handle.
            self._touch('deleted')
            self._wait_for('done')
            self.assertEqual(expected_after_delete[1], self._collect_value(sample_name, {'tenant': 't1'}))
        finally:
            self._touch('exit')
            stdout, stderr = worker.communicate(timeout=15)
            self.assertEqual(0, worker.returncode, stderr.decode('utf-8'))

    def test_counter_remove_stale_handle_collector(self):
        # before delete: 10; after the stale handle's next write: only the delta
        self._stale_handle_scenario('counter', 'm_total', (10, 1))

    def test_gauge_remove_stale_handle_collector(self):
        self._stale_handle_scenario('gauge', 'm', (10, 1))

    def test_histogram_remove_stale_handle_collector(self):
        self._stale_handle_scenario('histogram', 'm_count', (1, 1))
        self.assertEqual(4, self._collect_value('m_sum', {'tenant': 't1'}))
        self.assertEqual(1, self._collect_value('m_bucket', {'tenant': 't1', 'le': '5.0'}))

    def test_restart_and_recreate(self):
        self._run(_INC_SCRIPT, {'AMOUNT': '5'})
        self.assertEqual(5, self._collect_value('c_total', {'tenant': 't1'}))
        # A restarted worker continues accumulating in its own file.
        self._run(_INC_SCRIPT, {'AMOUNT': '3'})
        self.assertEqual(8, self._collect_value('c_total', {'tenant': 't1'}))
        # Tenant off-boarding: delete across all files.
        self._run(_REMOVE_COUNTER_SCRIPT)
        self.assertIsNone(self._collect_value('c_total', {'tenant': 't1'}))
        # A freshly started process re-creating the series starts from zero.
        self._run(_INC_SCRIPT, {'AMOUNT': '2'})
        self.assertEqual(2, self._collect_value('c_total', {'tenant': 't1'}))

    def test_repeated_remove(self):
        self._run(_INC_SCRIPT, {'AMOUNT': '7'})
        self.assertEqual(7, self._collect_value('c_total', {'tenant': 't1'}))
        # Repeated deletion and deletion of a non-existent series do not raise
        # and do not bring the data back.
        self._run(_REMOVE_COUNTER_SCRIPT)
        self.assertIsNone(self._collect_value('c_total', {'tenant': 't1'}))

    def test_concurrent_create(self):
        workers = [self._start(_INC_SCRIPT, {'AMOUNT': '25'}) for _ in range(4)]
        for worker in workers:
            stdout, stderr = worker.communicate(timeout=30)
            self.assertEqual(0, worker.returncode, stderr.decode('utf-8'))
        self.assertEqual(100, self._collect_value('c_total', {'tenant': 't1'}))
