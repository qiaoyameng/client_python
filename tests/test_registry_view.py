import unittest
from wsgiref.util import setup_testing_defaults

from prometheus_client import (
    CollectorRegistry, Counter, Gauge, generate_latest, Histogram, Info,
    make_wsgi_app, Summary,
)
from prometheus_client.core import (
    CounterMetricFamily, GaugeHistogramMetricFamily, GaugeMetricFamily,
    HistogramMetricFamily, InfoMetricFamily, Metric, SummaryMetricFamily,
)
from prometheus_client.registry import DuplicateTimeseries
from prometheus_client.registry_view import (
    ConstLabelCollisionError, DuplicateMetricNameError, RegistryView,
)
from prometheus_client.samples import BucketSpan, Exemplar, NativeHistogram
from prometheus_client.validation import (
    disable_legacy_validation, enable_legacy_validation,
)


def collect_by_name(source):
    return {m.name: m for m in source.collect()}


class TestRegistryView(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        self.counter = Counter('requests', 'Total requests', ['method'], registry=self.registry)
        self.counter.labels('get').inc(3)
        self.gauge = Gauge('temperature', 'Temperature', registry=self.registry)
        self.gauge.set(21)
        self.histogram = Histogram('latency_seconds', 'Latency', registry=self.registry)
        self.histogram.observe(0.4)
        self.summary = Summary('sizes', 'Sizes', registry=self.registry)
        self.summary.observe(5)
        self.info = Info('build', 'Build info', registry=self.registry)
        self.info.info({'version': '1.2.3'})
        self.view = RegistryView(self.registry, prefix='comp_', const_labels={'component': 'auth'})

    def test_counter_suffixes(self):
        metrics = collect_by_name(self.view)
        self.assertIn('comp_requests', metrics)
        sample_names = {s.name for s in metrics['comp_requests'].samples}
        self.assertEqual({'comp_requests_total', 'comp_requests_created'}, sample_names)
        total = [s for s in metrics['comp_requests'].samples if s.name == 'comp_requests_total'][0]
        self.assertEqual(3.0, total.value)
        self.assertEqual({'method': 'get', 'component': 'auth'}, total.labels)
        self.assertEqual('counter', metrics['comp_requests'].type)
        self.assertEqual('Total requests', metrics['comp_requests'].documentation)

    def test_gauge(self):
        metrics = collect_by_name(self.view)
        sample = metrics['comp_temperature'].samples[0]
        self.assertEqual('comp_temperature', sample.name)
        self.assertEqual({'component': 'auth'}, sample.labels)
        self.assertEqual(21.0, sample.value)

    def test_histogram_compound_samples(self):
        metrics = collect_by_name(self.view)
        sample_names = {s.name for s in metrics['comp_latency_seconds'].samples}
        self.assertIn('comp_latency_seconds_bucket', sample_names)
        self.assertIn('comp_latency_seconds_sum', sample_names)
        self.assertIn('comp_latency_seconds_count', sample_names)
        self.assertIn('comp_latency_seconds_created', sample_names)
        buckets = [s for s in metrics['comp_latency_seconds'].samples if s.name == 'comp_latency_seconds_bucket']
        self.assertTrue(all('le' in s.labels for s in buckets))
        self.assertTrue(all(s.labels['component'] == 'auth' for s in buckets))
        count = [s for s in metrics['comp_latency_seconds'].samples if s.name == 'comp_latency_seconds_count'][0]
        self.assertEqual(1.0, count.value)

    def test_summary_compound_samples(self):
        metrics = collect_by_name(self.view)
        sample_names = {s.name for s in metrics['comp_sizes'].samples}
        self.assertEqual({'comp_sizes_count', 'comp_sizes_sum', 'comp_sizes_created'}, sample_names)

    def test_info(self):
        metrics = collect_by_name(self.view)
        sample = metrics['comp_build'].samples[0]
        self.assertEqual('comp_build_info', sample.name)
        self.assertEqual({'version': '1.2.3', 'component': 'auth'}, sample.labels)
        self.assertEqual('info', metrics['comp_build'].type)

    def test_gaugehistogram(self):
        class CustomCollector:
            def collect(self):
                m = GaugeHistogramMetricFamily('queue_depth', 'Queue depth', buckets=[('1', 1.0), ('+Inf', 2.0)], gsum_value=3.0)
                yield m

        view = RegistryView(CustomCollector(), prefix='w_', const_labels={'worker': '0'})
        metrics = collect_by_name(view)
        sample_names = {s.name for s in metrics['w_queue_depth'].samples}
        self.assertEqual({'w_queue_depth_bucket', 'w_queue_depth_gsum', 'w_queue_depth_gcount'}, sample_names)

    def test_exemplar_preserved(self):
        counter = Counter('exemplar_counter', 'A counter with exemplars', registry=self.registry)
        counter.inc(2, {'trace_id': 'abc123'})
        metrics = collect_by_name(self.view)
        total = [s for s in metrics['comp_exemplar_counter'].samples if s.name == 'comp_exemplar_counter_total'][0]
        self.assertIsNotNone(total.exemplar)
        # Const labels are not added to exemplars.
        self.assertEqual({'trace_id': 'abc123'}, total.exemplar.labels)
        self.assertEqual(2.0, total.exemplar.value)

    def test_native_histogram_preserved(self):
        nh = NativeHistogram(1, 2.0, 0, 0.001, 0, pos_spans=[BucketSpan(0, 1)], pos_deltas=[1])

        class CustomCollector:
            def collect(self):
                m = Metric('native_hist', 'A native histogram', 'histogram')
                m.add_sample('native_hist_bucket', {}, 1.0, native_histogram=nh)
                yield m

        view = RegistryView(CustomCollector(), prefix='p_')
        metrics = collect_by_name(view)
        sample = metrics['p_native_hist'].samples[0]
        self.assertEqual('p_native_hist_bucket', sample.name)
        self.assertIs(nh, sample.native_histogram)

    def test_timestamp_preserved(self):
        class CustomCollector:
            def collect(self):
                m = GaugeMetricFamily('stamped', 'A stamped gauge')
                m.add_metric([], 1.0, timestamp=123.456)
                yield m

        view = RegistryView(CustomCollector(), prefix='p_')
        metrics = collect_by_name(view)
        self.assertEqual(123.456, metrics['p_stamped'].samples[0].timestamp)

    def test_const_label_collision_raises(self):
        Gauge('labelled', 'A gauge with labels', ['component'], registry=self.registry).labels('db').set(1)
        with self.assertRaises(ConstLabelCollisionError):
            list(self.view.collect())

    def test_duplicate_family_names_raise(self):
        class CustomCollector:
            def collect(self):
                yield GaugeMetricFamily('dup', 'First', value=1.0)
                yield GaugeMetricFamily('dup', 'Second', value=2.0)

        view = RegistryView(CustomCollector(), prefix='p_')
        with self.assertRaises(DuplicateMetricNameError):
            list(view.collect())

    def test_duplicate_sample_names_across_families_raise(self):
        class CustomCollector:
            def collect(self):
                yield CounterMetricFamily('thing', 'A counter', value=1.0)
                yield GaugeMetricFamily('thing_total', 'A gauge clashing with the counter', value=2.0)

        view = RegistryView(CustomCollector(), prefix='p_')
        with self.assertRaises(DuplicateMetricNameError):
            list(view.collect())

    def test_source_metrics_not_modified(self):
        before = list(self.registry.collect())
        original_samples = [s for m in before for s in m.samples]
        for metric in self.view.collect():
            for sample in metric.samples:
                self.assertNotIn(sample, original_samples)
        after = list(self.registry.collect())
        self.assertEqual(before, after)

    def test_source_sample_labels_not_modified(self):
        before = list(self.registry.collect())
        labels_before = [[dict(s.labels) for s in m.samples] for m in before]
        list(self.view.collect())
        labels_after = [[s.labels for s in m.samples] for m in self.registry.collect()]
        self.assertEqual(labels_before, labels_after)

    def test_two_views_same_registry(self):
        view_a = RegistryView(self.registry, prefix='a_', const_labels={'component': 'a'})
        view_b = RegistryView(self.registry, prefix='b_', const_labels={'component': 'b'})
        before = list(self.registry.collect())

        metrics_a = collect_by_name(view_a)
        metrics_b = collect_by_name(view_b)
        self.assertIn('a_requests', metrics_a)
        self.assertIn('b_requests', metrics_b)
        sample_a = [s for s in metrics_a['a_requests'].samples if s.name == 'a_requests_total'][0]
        sample_b = [s for s in metrics_b['b_requests'].samples if s.name == 'b_requests_total'][0]
        self.assertEqual('a', sample_a.labels['component'])
        self.assertEqual('b', sample_b.labels['component'])
        # Views do not share sample objects.
        self.assertIsNot(sample_a, sample_b)
        # The original registry is untouched.
        self.assertEqual(before, list(self.registry.collect()))

    def test_nested_views(self):
        inner = RegistryView(self.registry, prefix='inner_', const_labels={'component': 'auth'})
        outer = RegistryView(inner, prefix='outer_', const_labels={'dc': 'east'})
        metrics = collect_by_name(outer)
        sample = [s for s in metrics['outer_inner_requests'].samples if s.name == 'outer_inner_requests_total'][0]
        self.assertEqual({'method': 'get', 'component': 'auth', 'dc': 'east'}, sample.labels)
        # The inner view alone is unaffected by the outer one.
        inner_metrics = collect_by_name(inner)
        self.assertIn('inner_requests', inner_metrics)

    def test_nested_restricted_registry(self):
        inner = RegistryView(self.registry, prefix='inner_')
        outer = RegistryView(inner, prefix='outer_')
        restricted = outer.restricted_registry(['outer_inner_requests_total'])
        metrics = list(restricted.collect())
        self.assertEqual(1, len(metrics))
        self.assertEqual({'outer_inner_requests_total'}, {s.name for s in metrics[0].samples})

    def test_describe_matches_collect(self):
        class CustomCollector:
            def describe(self):
                return [
                    CounterMetricFamily('events', 'Events'),
                    HistogramMetricFamily('durations', 'Durations'),
                    InfoMetricFamily('meta', 'Meta'),
                ]

            def collect(self):
                c = CounterMetricFamily('events', 'Events', value=1.0)
                h = HistogramMetricFamily('durations', 'Durations', buckets=[('+Inf', 1.0)], sum_value=0.5)
                i = InfoMetricFamily('meta', 'Meta', value={'k': 'v'})
                return [c, h, i]

        view = RegistryView(CustomCollector(), prefix='p_', const_labels={'c': 'd'})
        described = {m.name for m in view.describe()}
        collected = {m.name for m in view.collect()}
        self.assertEqual({'p_events', 'p_durations', 'p_meta'}, described)
        self.assertEqual(described, collected)

    def test_describe_absent_when_source_lacks_it(self):
        class BareCollector:
            def collect(self):
                return []

        view = RegistryView(BareCollector(), prefix='p_')
        self.assertFalse(hasattr(view, 'describe'))

    def test_view_registered_into_gateway_registry(self):
        gateway = CollectorRegistry()
        gateway.register(RegistryView(self.registry, prefix='comp_', const_labels={'component': 'auth'}))
        metrics = collect_by_name(gateway)
        self.assertIn('comp_requests', metrics)

    def test_conflicting_views_rejected_by_gateway_registry(self):
        gateway = CollectorRegistry(auto_describe=True)
        gateway.register(RegistryView(self.registry, prefix='same_'))
        with self.assertRaises(DuplicateTimeseries):
            gateway.register(RegistryView(self.registry, prefix='same_'))

    def test_restricted_registry(self):
        restricted = self.view.restricted_registry(['comp_requests_total', 'comp_temperature'])
        metrics = collect_by_name(restricted)
        self.assertEqual({'comp_requests', 'comp_temperature'}, set(metrics))
        request_samples = {s.name for s in metrics['comp_requests'].samples}
        self.assertEqual({'comp_requests_total'}, request_samples)

    def test_restricted_registry_ignores_foreign_names(self):
        restricted = self.view.restricted_registry(['other_requests_total'])
        self.assertEqual([], list(restricted.collect()))

    def test_restricted_registry_without_source_support(self):
        class BareCollector:
            def collect(self):
                yield GaugeMetricFamily('one', 'One', value=1.0)
                yield GaugeMetricFamily('two', 'Two', value=2.0)

        view = RegistryView(BareCollector(), prefix='p_')
        restricted = view.restricted_registry(['p_one'])
        metrics = collect_by_name(restricted)
        self.assertEqual({'p_one'}, set(metrics))

    def test_generate_latest(self):
        output = generate_latest(self.view).decode('utf-8')
        self.assertIn('# TYPE comp_requests_total counter\n', output)
        self.assertIn('comp_requests_total{component=\"auth\",method=\"get\"} 3.0\n', output)
        self.assertIn('comp_build_info{component=\"auth\",version=\"1.2.3\"} 1.0\n', output)

    def test_wsgi_exposition(self):
        app = make_wsgi_app(self.view)
        environ = {}
        setup_testing_defaults(environ)
        captured = []

        def start_response(status, headers):
            captured.append(status)

        body = b''.join(app(environ, start_response)).decode('utf-8')
        self.assertEqual(['200 OK'], captured)
        self.assertIn('comp_requests_total{component=\"auth\",method=\"get\"} 3.0\n', body)

    def test_wsgi_exposition_with_name_params(self):
        app = make_wsgi_app(self.view)
        environ = {}
        setup_testing_defaults(environ)
        environ['QUERY_STRING'] = 'name[]=comp_requests_total'
        captured = []

        def start_response(status, headers):
            captured.append(status)

        body = b''.join(app(environ, start_response)).decode('utf-8')
        self.assertEqual(['200 OK'], captured)
        self.assertIn('comp_requests_total{component=\"auth\",method=\"get\"} 3.0\n', body)
        self.assertNotIn('comp_temperature', body)

    def test_invalid_prefix_rejected_with_legacy_validation(self):
        enable_legacy_validation()
        try:
            with self.assertRaises(ValueError):
                RegistryView(self.registry, prefix='9invalid')
        finally:
            disable_legacy_validation()

    def test_invalid_const_label_rejected(self):
        with self.assertRaises(ValueError):
            RegistryView(self.registry, const_labels={'__reserved': 'x'})

    def test_prefix_must_be_string(self):
        with self.assertRaises(TypeError):
            RegistryView(self.registry, prefix=42)

    def test_exemplar_and_labels_on_metric_families(self):
        class CustomCollector:
            def collect(self):
                c = CounterMetricFamily('fam_counter', 'A counter', labels=['l'])
                c.add_metric(['x'], 5.0, exemplar=Exemplar({'trace_id': 't'}, 5.0))
                yield c
                yield SummaryMetricFamily('fam_summary', 'A summary', count_value=2, sum_value=7.0)
                yield HistogramMetricFamily('fam_hist', 'A histogram', buckets=[('1', 1.0, Exemplar({'trace_id': 'u'}, 1.0)), ('+Inf', 2.0)], sum_value=3.0)

        view = RegistryView(CustomCollector(), prefix='v_', const_labels={'fixed': 'yes'})
        metrics = collect_by_name(view)
        counter_total = metrics['v_fam_counter'].samples[0]
        self.assertEqual('v_fam_counter_total', counter_total.name)
        self.assertEqual({'l': 'x', 'fixed': 'yes'}, counter_total.labels)
        self.assertEqual({'trace_id': 't'}, counter_total.exemplar.labels)
        summary_names = {s.name for s in metrics['v_fam_summary'].samples}
        self.assertEqual({'v_fam_summary_count', 'v_fam_summary_sum'}, summary_names)
        hist_buckets = [s for s in metrics['v_fam_hist'].samples if s.name == 'v_fam_hist_bucket']
        self.assertEqual({'trace_id': 'u'}, hist_buckets[0].exemplar.labels)
        self.assertEqual('yes', hist_buckets[0].labels['fixed'])


if __name__ == '__main__':
    unittest.main()
