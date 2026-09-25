import glob
import multiprocessing
import os
import shutil
import tempfile
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


def _subproc_inc(name, labelnames, labelvalues, amount):
    c = Counter(name, 'help', labelnames=labelnames, registry=None)
    c.labels(*labelvalues).inc(amount)


def _subproc_observe(name, labelnames, labelvalues, amount):
    h = Histogram(name, 'help', labelnames=labelnames, registry=None)
    h.labels(*labelvalues).observe(amount)


def _subproc_remove(kind, name, labelnames, labelvalues):
    cls = {'counter': Counter, 'gauge': Gauge, 'histogram': Histogram}[kind]
    metric = cls(name, 'help', labelnames=labelnames, registry=None)
    metric.remove(*labelvalues)


def _subproc_collect(name, labels, queue):
    registry = CollectorRegistry(support_collectors_without_names=True)
    MultiProcessCollector(registry)
    queue.put(registry.get_sample_value(name, labels))


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
            fs = [f for f in os.listdir(os.environ['PROMETHEUS_MULTIPROC_DIR'])
                  if not f.startswith('generations.')]
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

    def test_counter_remove_and_recreate(self):
        c = Counter('cr', 'help', labelnames=['l'], registry=None)
        stale_child = c.labels('a')
        stale_child.inc(2)
        c.labels('b').inc(3)
        self.assertEqual(2, self.registry.get_sample_value('cr_total', {'l': 'a'}))
        self.assertEqual(3, self.registry.get_sample_value('cr_total', {'l': 'b'}))

        c.remove('a')
        self.assertIsNone(self.registry.get_sample_value('cr_total', {'l': 'a'}))
        self.assertEqual(3, self.registry.get_sample_value('cr_total', {'l': 'b'}))

        # A stale handle to the removed child must not resurrect the series.
        stale_child.inc(5)
        self.assertIsNone(self.registry.get_sample_value('cr_total', {'l': 'a'}))

        # Re-creating the same series starts from scratch.
        c.labels('a').inc(1)
        self.assertEqual(1, self.registry.get_sample_value('cr_total', {'l': 'a'}))
        stale_child.inc(5)
        self.assertEqual(1, self.registry.get_sample_value('cr_total', {'l': 'a'}))

    def test_counter_remove_across_processes(self):
        c1 = Counter('crp', 'help', labelnames=['l'], registry=None)
        values.ValueClass = MultiProcessValue(lambda: 456)
        c2 = Counter('crp', 'help', labelnames=['l'], registry=None)
        c1.labels('a').inc(1)
        c2.labels('a').inc(2)
        self.assertEqual(3, self.registry.get_sample_value('crp_total', {'l': 'a'}))
        # Removal covers the files of all processes, not just the current one.
        c2.remove('a')
        self.assertIsNone(self.registry.get_sample_value('crp_total', {'l': 'a'}))

    def test_counter_clear(self):
        c = Counter('cc', 'help', labelnames=['l'], registry=None)
        stale_child = c.labels('a')
        stale_child.inc(1)
        c.labels('b').inc(2)
        c.clear()
        self.assertIsNone(self.registry.get_sample_value('cc_total', {'l': 'a'}))
        self.assertIsNone(self.registry.get_sample_value('cc_total', {'l': 'b'}))
        stale_child.inc(1)
        self.assertIsNone(self.registry.get_sample_value('cc_total', {'l': 'a'}))
        c.labels('a').inc(3)
        self.assertEqual(3, self.registry.get_sample_value('cc_total', {'l': 'a'}))

    def test_counter_remove_by_labels(self):
        c = Counter('crl', 'help', labelnames=['a', 'b'], registry=None)
        c.labels('1', 'x').inc(1)
        c.labels('1', 'y').inc(2)
        c.labels('2', 'x').inc(3)
        c.remove_by_labels({'a': '1'})
        self.assertIsNone(self.registry.get_sample_value('crl_total', {'a': '1', 'b': 'x'}))
        self.assertIsNone(self.registry.get_sample_value('crl_total', {'a': '1', 'b': 'y'}))
        self.assertEqual(3, self.registry.get_sample_value('crl_total', {'a': '2', 'b': 'x'}))

    def test_histogram_remove_treats_samples_as_one_series(self):
        h = Histogram('hr', 'help', labelnames=['l'], registry=None)
        stale_child = h.labels('x')
        stale_child.observe(1)
        self.assertEqual(1, self.registry.get_sample_value('hr_count', {'l': 'x'}))
        self.assertEqual(1, self.registry.get_sample_value('hr_bucket', {'l': 'x', 'le': '5.0'}))

        h.remove('x')
        self.assertIsNone(self.registry.get_sample_value('hr_count', {'l': 'x'}))
        self.assertIsNone(self.registry.get_sample_value('hr_sum', {'l': 'x'}))
        self.assertIsNone(self.registry.get_sample_value('hr_bucket', {'l': 'x', 'le': '5.0'}))

        # A stale handle must not resurrect any of the bucket/sum/count samples.
        stale_child.observe(5)
        self.assertIsNone(self.registry.get_sample_value('hr_count', {'l': 'x'}))
        self.assertIsNone(self.registry.get_sample_value('hr_bucket', {'l': 'x', 'le': '5.0'}))

        h.labels('x').observe(2)
        self.assertEqual(1, self.registry.get_sample_value('hr_count', {'l': 'x'}))
        self.assertEqual(2, self.registry.get_sample_value('hr_sum', {'l': 'x'}))
        self.assertEqual(1, self.registry.get_sample_value('hr_bucket', {'l': 'x', 'le': '5.0'}))

    def test_gauge_remove_all_modes(self):
        for mode in ('all', 'min', 'max', 'sum', 'mostrecent',
                     'liveall', 'livemin', 'livemax', 'livesum', 'livemostrecent'):
            name = 'g_' + mode
            labels = {'l': 'x', 'pid': '123'} if mode in ('all', 'liveall') else {'l': 'x'}
            values.ValueClass = MultiProcessValue(lambda: 123)
            g1 = Gauge(name, 'help', labelnames=['l'], registry=None, multiprocess_mode=mode)
            g1_child = g1.labels('x')
            values.ValueClass = MultiProcessValue(lambda: 456)
            g2 = Gauge(name, 'help', labelnames=['l'], registry=None, multiprocess_mode=mode)
            g1_child.set(1)
            stale_child = g2.labels('x')
            stale_child.set(2)
            self.assertIsNotNone(self.registry.get_sample_value(name, labels), 'mode ' + mode)

            g1.remove('x')
            self.assertIsNone(self.registry.get_sample_value(name, labels), 'mode ' + mode)
            stale_child.set(5)
            self.assertIsNone(self.registry.get_sample_value(name, labels), 'stale write in mode ' + mode)

            values.ValueClass = MultiProcessValue(lambda: 123)
            g1.labels('x').set(7)
            self.assertEqual(7, self.registry.get_sample_value(name, labels), 'recreate in mode ' + mode)

    def test_repeated_remove(self):
        c = Counter('crr', 'help', labelnames=['l'], registry=None)
        c.labels('a').inc(1)
        c.remove('a')
        # Removing an already removed series is a no-op and does not fail.
        c.remove('a')
        self.assertIsNone(self.registry.get_sample_value('crr_total', {'l': 'a'}))
        c.labels('a').inc(2)
        self.assertEqual(2, self.registry.get_sample_value('crr_total', {'l': 'a'}))
        c.remove('a')
        c.remove('a')
        self.assertIsNone(self.registry.get_sample_value('crr_total', {'l': 'a'}))
        c.labels('a').inc(3)
        self.assertEqual(3, self.registry.get_sample_value('crr_total', {'l': 'a'}))

    def test_legacy_files_without_generation(self):
        # Simulate a data file written by an old client: 4-element keys and
        # no generations.json. Such files must remain fully readable.
        d = mmap_dict.MmapedDict(os.path.join(self.tempdir, 'counter_999.db'))
        key = mmap_dict.mmap_key('legacy_c', 'legacy_c_total', ['t'], ['x'], 'help')
        d.write_value(key, 7.0, 0.0)
        d.close()
        self.assertEqual(7, self.registry.get_sample_value('legacy_c_total', {'t': 'x'}))

        # Removal covers legacy entries too.
        c = Counter('legacy_c', 'help', labelnames=['t'], registry=None)
        c.remove('x')
        self.assertIsNone(self.registry.get_sample_value('legacy_c_total', {'t': 'x'}))

        # Re-creating the series starts from scratch.
        c.labels('x').inc(2)
        self.assertEqual(2, self.registry.get_sample_value('legacy_c_total', {'t': 'x'}))
    
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

    def test_tombstoned_entries_are_skipped(self):
        self.d.write_value('abc', 123.0, 987.0)
        self.d.write_value('def', 42.0, 765.0)
        # Tombstone 'abc' by negating its key length (first entry, pos 8).
        mmap_dict._pack_integer(self.d._m, 8, -3)
        self.assertEqual([('def', 42.0, 765.0)], list(self.d.read_all_values()))
        # A reopened file (e.g. after a process restart) still skips it.
        self.d.close()
        self.d = mmap_dict.MmapedDict(self.tempfile)
        self.assertEqual([('def', 42.0, 765.0)], list(self.d.read_all_values()))

    def test_stale_write_to_tombstoned_entry_is_not_resurrected(self):
        self.d.write_value('abc', 123.0, 987.0)
        mmap_dict._pack_integer(self.d._m, 8, -3)
        # This dict still caches the position of the tombstoned entry, like a
        # process holding a deleted child. Writing there must stay invisible.
        self.d.write_value('abc', 5.0, 0.0)
        self.assertEqual([], list(self.d.read_all_values()))
        # Re-initializing the key (as a re-created series would) appends a
        # fresh entry instead of reviving the tombstoned one.
        self.d.close()
        self.d = mmap_dict.MmapedDict(self.tempfile)
        self.assertEqual((0.0, 0.0), self.d.read_value('abc'))
        self.assertEqual([('abc', 0.0, 0.0)], list(self.d.read_all_values()))

    def tearDown(self):
        self.d.close()
        os.unlink(self.tempfile)


class TestGenerationStore(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_default_generation_is_zero(self):
        store = mmap_dict.GenerationStore(self.tempdir)
        self.assertEqual({}, store.read_all())
        with store.locked():
            self.assertEqual(0, store.get_locked('some-series'))

    def test_delete_series_bumps_generation_and_tombstones(self):
        key = mmap_dict.mmap_key('m', 'm_total', ['l'], ['a'], 'help')
        other_key = mmap_dict.mmap_key('m', 'm_total', ['l'], ['b'], 'help')
        for pid in (1, 2):
            d = mmap_dict.MmapedDict(os.path.join(self.tempdir, 'counter_%d.db' % pid))
            d.write_value(key, 5.0, 0.0)
            d.write_value(other_key, 6.0, 0.0)
            d.close()

        series_key = mmap_dict.mmap_series_key('m', {'l': 'a'})
        removed = mmap_dict.delete_series(self.tempdir, 'm', lambda labels: labels.get('l') == 'a')
        self.assertEqual([series_key], removed)
        self.assertEqual({series_key: 1}, mmap_dict.GenerationStore(self.tempdir).read_all())
        for pid in (1, 2):
            self.assertEqual(
                [(other_key, 6.0, 0.0)],
                [(k, v, ts) for k, v, ts, _ in mmap_dict.MmapedDict.read_all_values_from_file(
                    os.path.join(self.tempdir, 'counter_%d.db' % pid))])

        # Deleting again finds no live entries and leaves the generation alone.
        self.assertEqual([], mmap_dict.delete_series(
            self.tempdir, 'm', lambda labels: labels.get('l') == 'a'))
        self.assertEqual({series_key: 1}, mmap_dict.GenerationStore(self.tempdir).read_all())

        # A re-created (generation 1) series can be deleted again.
        d = mmap_dict.MmapedDict(os.path.join(self.tempdir, 'counter_1.db'))
        key_v1 = mmap_dict.mmap_key_with_generation('m', 'm_total', ['l'], ['a'], 'help', 1)
        d.write_value(key_v1, 1.0, 0.0)
        d.close()
        self.assertEqual([series_key], mmap_dict.delete_series(
            self.tempdir, 'm', lambda labels: labels.get('l') == 'a'))
        self.assertEqual({series_key: 2}, mmap_dict.GenerationStore(self.tempdir).read_all())

    def test_parse_mmap_key_accepts_legacy_and_generation_keys(self):
        legacy = mmap_dict.mmap_key('m', 'm_total', ['l'], ['a'], 'help')
        self.assertEqual(('m', 'm_total', {'l': 'a'}, 'help', 0),
                         mmap_dict.parse_mmap_key(legacy))
        tagged = mmap_dict.mmap_key_with_generation('m', 'm_total', ['l'], ['a'], 'help', 3)
        self.assertEqual(('m', 'm_total', {'l': 'a'}, 'help', 3),
                         mmap_dict.parse_mmap_key(tagged))
        # Generation 0 keeps the legacy 4-element format.
        self.assertEqual(legacy, mmap_dict.mmap_key_with_generation('m', 'm_total', ['l'], ['a'], 'help', 0))

@unittest.skipUnless(hasattr(os, 'fork'), 'fork is required for subprocess tests')
class TestMultiProcessRemovalSubprocesses(unittest.TestCase):
    """End-to-end remove/clear tests using real (forked) subprocesses."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = self.tempdir
        values.ValueClass = get_value_class()
        self.ctx = multiprocessing.get_context('fork')

    def tearDown(self):
        del os.environ['PROMETHEUS_MULTIPROC_DIR']
        values.close_all_multiprocess_files()
        values.ValueClass = MutexValue
        shutil.rmtree(self.tempdir)

    def _run(self, target, args=()):
        p = self.ctx.Process(target=target, args=args)
        p.start()
        p.join(30)
        if p.is_alive():
            p.terminate()
            p.join()
        self.assertEqual(0, p.exitcode)

    def _collect(self, name, labels=None):
        # Collect in a fresh subprocess, like a dedicated exporter would.
        queue = self.ctx.Queue()
        p = self.ctx.Process(target=_subproc_collect, args=(name, labels, queue))
        p.start()
        p.join(30)
        if p.is_alive():
            p.terminate()
            p.join()
        self.assertEqual(0, p.exitcode)
        return queue.get(timeout=5)

    def test_stale_handle_writer_deleter_collector(self):
        counter = Counter('tenant_requests', 'help', labelnames=['tenant'], registry=None)
        child = counter.labels('acme')
        child.inc(5)
        self.assertEqual(5, self._collect('tenant_requests_total', {'tenant': 'acme'}))

        ready = self.ctx.Event()
        done = self.ctx.Event()

        def stale_writer():
            # The first write after the fork rebinds the inherited child to a
            # fresh per-process file.
            child.inc(1)
            ready.set()
            done.wait(30)
            # The series has been deleted by now; this stale handle must not
            # resurrect the old values.
            child.inc(3)

        writer = self.ctx.Process(target=stale_writer)
        writer.start()
        self.assertTrue(ready.wait(30))
        self.assertEqual(6, self._collect('tenant_requests_total', {'tenant': 'acme'}))

        # A second process deletes the series across the whole directory.
        self._run(_subproc_remove, ('counter', 'tenant_requests', ('tenant',), ('acme',)))
        self.assertIsNone(self._collect('tenant_requests_total', {'tenant': 'acme'}))

        done.set()
        writer.join(30)
        self.assertEqual(0, writer.exitcode)
        self.assertIsNone(self._collect('tenant_requests_total', {'tenant': 'acme'}))

        # Re-creating the series starts from zero, unaffected by stale writes.
        self._run(_subproc_inc, ('tenant_requests', ('tenant',), ('acme',), 2))
        self.assertEqual(2, self._collect('tenant_requests_total', {'tenant': 'acme'}))

    def test_histogram_remove_across_processes(self):
        self._run(_subproc_observe, ('mp_h', ('t',), ('x',), 3))
        self.assertEqual(1, self._collect('mp_h_count', {'t': 'x'}))
        self.assertEqual(3, self._collect('mp_h_sum', {'t': 'x'}))

        self._run(_subproc_remove, ('histogram', 'mp_h', ('t',), ('x',)))
        self.assertIsNone(self._collect('mp_h_count', {'t': 'x'}))
        self.assertIsNone(self._collect('mp_h_sum', {'t': 'x'}))
        self.assertIsNone(self._collect('mp_h_bucket', {'t': 'x', 'le': '+Inf'}))

        self._run(_subproc_observe, ('mp_h', ('t',), ('x',), 2))
        self.assertEqual(1, self._collect('mp_h_count', {'t': 'x'}))
        self.assertEqual(2, self._collect('mp_h_sum', {'t': 'x'}))

    def test_restart_after_remove(self):
        self._run(_subproc_inc, ('restart_c', ('t',), ('x',), 5))
        self.assertEqual(5, self._collect('restart_c_total', {'t': 'x'}))

        self._run(_subproc_remove, ('counter', 'restart_c', ('t',), ('x',)))
        self.assertIsNone(self._collect('restart_c_total', {'t': 'x'}))

        # A restarted process re-creating the series starts from zero and
        # does not resurrect the values of the previous incarnation.
        self._run(_subproc_inc, ('restart_c', ('t',), ('x',), 2))
        self.assertEqual(2, self._collect('restart_c_total', {'t': 'x'}))

    def test_repeated_remove(self):
        self._run(_subproc_inc, ('rep_c', ('t',), ('x',), 5))
        self._run(_subproc_remove, ('counter', 'rep_c', ('t',), ('x',)))
        # Removing an already removed series is a no-op and does not fail.
        self._run(_subproc_remove, ('counter', 'rep_c', ('t',), ('x',)))
        self.assertIsNone(self._collect('rep_c_total', {'t': 'x'}))

        self._run(_subproc_inc, ('rep_c', ('t',), ('x',), 3))
        self.assertEqual(3, self._collect('rep_c_total', {'t': 'x'}))
        self._run(_subproc_remove, ('counter', 'rep_c', ('t',), ('x',)))
        self._run(_subproc_remove, ('counter', 'rep_c', ('t',), ('x',)))
        self.assertIsNone(self._collect('rep_c_total', {'t': 'x'}))

    def test_concurrent_creation(self):
        procs = 4
        increments = 50
        barrier = self.ctx.Barrier(procs)

        def creator():
            c = Counter('conc_c', 'help', labelnames=['t'], registry=None)
            child = c.labels('x')
            barrier.wait()
            for _ in range(increments):
                child.inc(1)

        ps = [self.ctx.Process(target=creator) for _ in range(procs)]
        for p in ps:
            p.start()
        for p in ps:
            p.join(30)
        for p in ps:
            self.assertEqual(0, p.exitcode)
        self.assertEqual(procs * increments, self._collect('conc_c_total', {'t': 'x'}))

    def test_concurrent_remove_and_recreate(self):
        iterations = 100

        def creator():
            c = Counter('race_c', 'help', labelnames=['t'], registry=None)
            for _ in range(iterations):
                c.labels('x').inc(1)
                c.remove('x')

        def deleter():
            c = Counter('race_c', 'help', labelnames=['t'], registry=None)
            for _ in range(iterations):
                c.remove('x')

        ps = [self.ctx.Process(target=creator) for _ in range(2)]
        ps.append(self.ctx.Process(target=deleter))
        for p in ps:
            p.start()
        for p in ps:
            p.join(60)
        for p in ps:
            self.assertEqual(0, p.exitcode)

        # Once everything settles, a final remove followed by a single
        # re-create must yield exactly the new value.
        self._run(_subproc_remove, ('counter', 'race_c', ('t',), ('x',)))
        self._run(_subproc_inc, ('race_c', ('t',), ('x',), 7))
        self.assertEqual(7, self._collect('race_c_total', {'t': 'x'}))


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
