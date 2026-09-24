import unittest
from wsgiref.util import setup_testing_defaults

from prometheus_client import (
    CollectorRegistry, ConstLabelCollisionError, Counter, DuplicateMetricError,
    Gauge, generate_latest, make_wsgi_app, RegistryView,
)
from prometheus_client.core import (
    CounterMetricFamily, Exemplar, GaugeMetricFamily, HistogramMetricFamily,
    InfoMetricFamily, Metric, SummaryMetricFamily,
)
from prometheus_client.samples import BucketSpan, NativeHistogram


def collect_map(source):
    return {m.name: m for m in source.collect()}


class StaticCollector:
    def __init__(self, *metrics):
        self._metrics = metrics

    def collect(self):
        return list(self._metrics)


class TestRegistryViewTransform(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()

    def test_counter_prefix_and_suffixes(self):
        c = CounterMetricFamily('requests', 'Total requests', value=3, created=100.0)
        self.registry.register(StaticCollector(c))
        view = RegistryView(self.registry, prefix='component_')
        metrics = collect_map(view)
        self.assertIn('component_requests', metrics)
        self.assertEqual('counter', metrics['component_requests'].type)
        names = {s.name for s in metrics['component_requests'].samples}
        self.assertEqual({'component_requests_total', 'component_requests_created'}, names)
        self.assertEqual(3, view.get_sample_value('component_requests_total'))
        self.assertEqual(100.0, view.get_sample_value('component_requests_created'))

    def test_const_labels_added(self):
        g = GaugeMetricFamily('temperature', 'Temp', labels=['room'])
        g.add_metric(['b'], 21)
        self.registry.register(StaticCollector(g))
        view = RegistryView(self.registry, prefix='a_', const_labels={'component': 'sensor', 'dc': 'east'})
        sample = list(view.collect())[0].samples[0]
        self.assertEqual({'room': 'b', 'component': 'sensor', 'dc': 'east'}, sample.labels)

    def test_histogram_compound_samples_and_exemplar(self):
        exemplar = Exemplar({'trace_id': 'abc'}, 0.6, 123.0)
        h = HistogramMetricFamily('latency', 'Latency',
                                  buckets=[('0.5', 1, exemplar), ('+Inf', 2)],
                                  sum_value=0.7)
        self.registry.register(StaticCollector(h))
        view = RegistryView(self.registry, prefix='api_', const_labels={'app': 'gw'})
        metrics = collect_map(view)
        samples = metrics['api_latency'].samples
        by_name = {}
        for s in samples:
            by_name.setdefault(s.name, []).append(s)
        self.assertEqual({'api_latency_bucket', 'api_latency_count', 'api_latency_sum'}, set(by_name))
        buckets = by_name['api_latency_bucket']
        self.assertEqual([{'le': '0.5', 'app': 'gw'}, {'le': '+Inf', 'app': 'gw'}],
                         [s.labels for s in buckets])
        # Exemplar passed through untouched.
        self.assertEqual(exemplar, buckets[0].exemplar)
        self.assertEqual(exemplar.labels, {'trace_id': 'abc'})
        self.assertEqual(2, by_name['api_latency_count'][0].value)
        self.assertEqual(0.7, by_name['api_latency_sum'][0].value)

    def test_summary_compound_samples(self):
        s = SummaryMetricFamily('sizes', 'Sizes', count_value=5, sum_value=12.5)
        self.registry.register(StaticCollector(s))
        view = RegistryView(self.registry, prefix='x_')
        names = {sample.name for sample in list(view.collect())[0].samples}
        self.assertEqual({'x_sizes_count', 'x_sizes_sum'}, names)

    def test_info_metric(self):
        i = InfoMetricFamily('build', 'Build info', value={'version': '1.2.3'})
        self.registry.register(StaticCollector(i))
        view = RegistryView(self.registry, prefix='svc_', const_labels={'component': 'api'})
        metrics = collect_map(view)
        self.assertIn('svc_build', metrics)
        self.assertEqual('info', metrics['svc_build'].type)
        sample = metrics['svc_build'].samples[0]
        self.assertEqual('svc_build_info', sample.name)
        self.assertEqual({'version': '1.2.3', 'component': 'api'}, sample.labels)
        self.assertEqual(1, sample.value)

    def test_native_histogram_passthrough(self):
        nh = NativeHistogram(
            count_value=4, sum_value=10.0, schema=0, zero_threshold=0.001, zero_count=1,
            pos_spans=[BucketSpan(0, 2)], pos_deltas=[1, 2],
            nh_exemplars=[Exemplar({'trace_id': 'xyz'}, 3.0)],
        )
        m = Metric('nh', 'Native histogram', 'histogram')
        m.add_sample('nh', {'job': 'j'}, 4, exemplar=None, native_histogram=nh)
        self.registry.register(StaticCollector(m))
        view = RegistryView(self.registry, prefix='p_', const_labels={'component': 'c'})
        sample = list(view.collect())[0].samples[0]
        self.assertEqual('p_nh', sample.name)
        self.assertEqual({'job': 'j', 'component': 'c'}, sample.labels)
        # Native histogram passed through by identity, not copied or rebuilt.
        self.assertIs(nh, sample.native_histogram)

    def test_original_objects_not_mutated(self):
        c = CounterMetricFamily('hits', 'Hits', labels=['path'])
        c.add_metric(['/a'], 7)
        self.registry.register(StaticCollector(c))
        before = list(self.registry.collect())
        view = RegistryView(self.registry, prefix='z_', const_labels={'component': 'web'})
        viewed = list(view.collect())
        after = list(self.registry.collect())
        # Original registry output is item-by-item unchanged.
        self.assertEqual(before, after)
        self.assertEqual('hits', before[0].name)
        self.assertEqual('hits_total', before[0].samples[0].name)
        self.assertEqual({'path': '/a'}, before[0].samples[0].labels)
        # View does not share mutable label dicts with the source samples.
        self.assertIsNot(viewed[0].samples[0].labels, before[0].samples[0].labels)
        # Source metric object untouched.
        self.assertEqual('hits', c.name)
        self.assertEqual({'path': '/a'}, c.samples[0].labels)

    def test_const_label_collision_raises(self):
        g = GaugeMetricFamily('g', 'A gauge', labels=['component'])
        g.add_metric(['existing'], 1)
        self.registry.register(StaticCollector(g))
        view = RegistryView(self.registry, prefix='a_', const_labels={'component': 'x'})
        with self.assertRaises(ConstLabelCollisionError):
            list(view.collect())

    def test_duplicate_metric_names_raise(self):
        class DupCollector:
            def collect(self):
                m1 = GaugeMetricFamily('same', 'one', value=1)
                m2 = GaugeMetricFamily('same', 'two', value=2)
                return [m1, m2]

        view = RegistryView(DupCollector(), prefix='p_')
        with self.assertRaises(DuplicateMetricError):
            list(view.collect())

    def test_invalid_const_label_name_raises(self):
        with self.assertRaises(ValueError):
            RegistryView(self.registry, const_labels={'__reserved': 'x'})


class TestRegistryViewDescribeAndRegistration(unittest.TestCase):
    def test_describe_matches_collect_names(self):
        registry = CollectorRegistry()
        Counter('c', 'A counter', registry=registry)
        Gauge('g', 'A gauge', registry=registry)
        view = RegistryView(registry, prefix='v_')
        describe_names = {s.name for m in view.describe() for s in m.samples}
        collect_names = {s.name for m in view.collect() for s in m.samples}
        self.assertEqual(describe_names, collect_names)
        self.assertIn('v_c_total', describe_names)

    def test_views_register_into_gateway_registry(self):
        reg_a = CollectorRegistry()
        Counter('requests', 'Requests', registry=reg_a).inc()
        reg_b = CollectorRegistry()
        Counter('requests', 'Requests', registry=reg_b).inc(2)

        gateway = CollectorRegistry()
        gateway.register(RegistryView(reg_a, prefix='a_', const_labels={'component': 'a'}))
        gateway.register(RegistryView(reg_b, prefix='b_', const_labels={'component': 'b'}))

        self.assertEqual(1, gateway.get_sample_value('a_requests_total', {'component': 'a'}))
        self.assertEqual(2, gateway.get_sample_value('b_requests_total', {'component': 'b'}))
        output = generate_latest(gateway).decode()
        self.assertIn('a_requests_total{component="a"} 1.0', output)
        self.assertIn('b_requests_total{component="b"} 2.0', output)

    def test_two_views_wrap_same_registry(self):
        registry = CollectorRegistry()
        Counter('ops', 'Operations', registry=registry).inc(5)
        view1 = RegistryView(registry, prefix='one_', const_labels={'view': 'one'})
        view2 = RegistryView(registry, prefix='two_', const_labels={'view': 'two'})
        self.assertEqual(5, view1.get_sample_value('one_ops_total', {'view': 'one'}))
        self.assertEqual(5, view2.get_sample_value('two_ops_total', {'view': 'two'}))
        # Source registry unaffected.
        self.assertEqual(5, registry.get_sample_value('ops_total'))

    def test_nested_views(self):
        registry = CollectorRegistry()
        Counter('ops', 'Operations', registry=registry).inc(3)
        inner = RegistryView(registry, prefix='inner_', const_labels={'layer': 'inner'})
        outer = RegistryView(inner, prefix='outer_', const_labels={'layer2': 'outer'})
        metrics = collect_map(outer)
        self.assertIn('outer_inner_ops', metrics)
        sample = metrics['outer_inner_ops'].samples[0]
        self.assertEqual('outer_inner_ops_total', sample.name)
        self.assertEqual({'layer': 'inner', 'layer2': 'outer'}, sample.labels)
        # Inner view and source still report their own names.
        self.assertEqual(3, inner.get_sample_value('inner_ops_total', {'layer': 'inner'}))
        self.assertEqual(3, registry.get_sample_value('ops_total'))

    def test_nested_view_const_label_collision(self):
        registry = CollectorRegistry()
        Gauge('g', 'A gauge', registry=registry)
        inner = RegistryView(registry, const_labels={'component': 'inner'})
        outer = RegistryView(inner, const_labels={'component': 'outer'})
        with self.assertRaises(ConstLabelCollisionError):
            list(outer.collect())


class TestRestrictedRegistryView(unittest.TestCase):
    def test_restricted_collect_maps_back_to_source(self):
        registry = CollectorRegistry()
        Counter('alpha', 'Alpha', registry=registry).inc(2)
        Counter('beta', 'Beta', registry=registry).inc(4)
        view = RegistryView(registry, prefix='v_', const_labels={'component': 'c'})
        restricted = view.restricted_registry(['v_alpha_total'])
        metrics = list(restricted.collect())
        names = {s.name for m in metrics for s in m.samples}
        self.assertEqual({'v_alpha_total'}, names)
        self.assertEqual(2, list(restricted.collect())[0].samples[0].value)
        # Unknown / non-prefixed names yield nothing.
        self.assertEqual([], list(view.restricted_registry(['other_total']).collect()))

    def test_restricted_view_of_plain_collector(self):
        class Plain:
            def collect(self):
                return [GaugeMetricFamily('one', 'one', value=1),
                        GaugeMetricFamily('two', 'two', value=2)]

        view = RegistryView(Plain(), prefix='p_')
        restricted = view.restricted_registry(['p_two'])
        names = {s.name for m in restricted.collect() for s in m.samples}
        self.assertEqual({'p_two'}, names)

    def test_restricted_nested_view(self):
        registry = CollectorRegistry()
        Counter('alpha', 'Alpha', registry=registry).inc(9)
        Counter('beta', 'Beta', registry=registry).inc(1)
        view = RegistryView(RegistryView(registry, prefix='i_'), prefix='o_')
        restricted = view.restricted_registry(['o_i_alpha_total'])
        names = {s.name for m in restricted.collect() for s in m.samples}
        self.assertEqual({'o_i_alpha_total'}, names)

    def test_restricted_generate_latest(self):
        registry = CollectorRegistry()
        Counter('alpha', 'Alpha', registry=registry).inc(2)
        Counter('beta', 'Beta', registry=registry).inc(4)
        view = RegistryView(registry, prefix='v_')
        output = generate_latest(view.restricted_registry(['v_beta_total'])).decode()
        self.assertIn('v_beta_total 4.0', output)
        self.assertNotIn('alpha', output)


class TestRegistryViewExposition(unittest.TestCase):
    def test_generate_latest(self):
        registry = CollectorRegistry()
        Counter('requests', 'Requests', registry=registry).inc(3)
        view = RegistryView(registry, prefix='svc_', const_labels={'component': 'api'})
        output = generate_latest(view).decode()
        self.assertIn('# HELP svc_requests_total Requests', output)
        self.assertIn('# TYPE svc_requests_total counter', output)
        self.assertIn('svc_requests_total{component="api"} 3.0', output)

    def test_make_wsgi_app(self):
        registry = CollectorRegistry()
        Counter('requests', 'Requests', registry=registry).inc(6)
        view = RegistryView(registry, prefix='svc_', const_labels={'component': 'api'})
        app = make_wsgi_app(view)
        environ = {}
        setup_testing_defaults(environ)
        captured = {}

        def start_response(status, headers):
            captured['status'] = status

        body = b''.join(app(environ, start_response)).decode()
        self.assertEqual('200 OK', captured['status'])
        self.assertIn('svc_requests_total{component="api"} 6.0', body)


if __name__ == '__main__':
    unittest.main()
