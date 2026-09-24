from typing import Any, Dict, Iterable, Optional, Set

from .metrics_core import Metric
from .registry import Collector
from .samples import Sample
from .validation import (
    _is_valid_legacy_metric_name, _validate_labelname, get_legacy_validation,
)


class ConstLabelCollisionError(ValueError):
    """Raised when a view's const label collides with a sample's own label."""


class DuplicateMetricNameError(ValueError):
    """Raised when a view transformation maps multiple metrics to the same name."""


# Sample name suffixes that belong to a metric family of a given type. The
# prefix of a view is applied to the family name, keeping these suffixes in
# place, so that e.g. a counter "requests" viewed with prefix "api_" yields
# "api_requests_total" with the "_total" suffix preserved.
_TYPE_SUFFIXES = {
    'counter': ('_total', '_created'),
    'histogram': ('_bucket', '_sum', '_count', '_created'),
    'summary': ('_sum', '_count', '_created'),
    'gaugehistogram': ('_bucket', '_gsum', '_gcount'),
    'info': ('_info',),
}


class RegistryView:
    """A read-only, transforming view over a collector or registry.

    A view wraps any object with a no-argument collect method (a
    CollectorRegistry, another RegistryView, or any custom
    collector) and re-exposes its metrics with a name prefix and a set of
    const (fixed) labels applied. This allows a gateway to aggregate
    several components, each keeping its own registry, behind a single
    exposition endpoint.

    The wrapped source is never modified, and neither are the Metric
    or Sample objects it returns: every collect() produces freshly
    built copies, so multiple views (including nested views) can wrap the
    same source independently.

    The view can be used anywhere a registry is accepted, e.g.
    generate_latest(view), make_wsgi_app(view), or it can be
    registered into another CollectorRegistry.

    Collisions are never resolved silently:
      * if a const label name already exists on a sample,
        ConstLabelCollisionError is raised;
      * if the transformation maps two metric families or two samples of
        different families onto the same name,
        DuplicateMetricNameError is raised.
    """

    def __init__(self,
                 source: Collector,
                 prefix: str = '',
                 const_labels: Optional[Dict[str, str]] = None,
                 ):
        if not isinstance(prefix, str):
            raise TypeError('prefix must be a string')
        if get_legacy_validation() and prefix and not _is_valid_legacy_metric_name(prefix):
            raise ValueError('invalid metric name prefix ' + prefix)
        try:
            prefix.encode('utf-8')
        except UnicodeDecodeError:
            raise ValueError('invalid metric name prefix ' + prefix)
        const_labels = dict(const_labels) if const_labels else {}
        for label_name in const_labels:
            _validate_labelname(label_name)
        self._source = source
        self._prefix = prefix
        self._const_labels = const_labels

    def collect(self) -> Iterable[Metric]:
        """Yields transformed copies of the metrics from the source."""
        family_names: Set[str] = set()
        sample_owners: Dict[str, str] = {}
        for metric in self._source.collect():
            transformed = self._transform_metric(metric)
            if transformed.name in family_names:
                raise DuplicateMetricNameError(
                    'Transformation produced duplicate metric family name: ' + transformed.name)
            family_names.add(transformed.name)
            for sample in transformed.samples:
                owner = sample_owners.setdefault(sample.name, transformed.name)
                if owner != transformed.name:
                    raise DuplicateMetricNameError(
                        'Transformation produced duplicate sample name {!r} '
                        'in families {!r} and {!r}'.format(sample.name, owner, transformed.name))
            yield transformed

    def restricted_registry(self, names: Iterable[str]) -> "RegistryView":
        """Returns a view that only collects the given (transformed) names.

        Names are given in the view's namespace (i.e. with the prefix
        applied); they are mapped back to the source's namespace so that
        only the originating collectors are invoked.
        """
        source_names = [name[len(self._prefix):] for name in names if name.startswith(self._prefix)]
        restrict = getattr(self._source, 'restricted_registry', None)
        restricted = restrict(source_names) if restrict is not None else _FilteringCollector(source_names, self._source)
        return RegistryView(restricted, prefix=self._prefix, const_labels=self._const_labels)

    def __getattr__(self, name: str) -> Any:
        # Only expose describe() if the source does, so that the view
        # behaves like its source when registered into a CollectorRegistry.
        if name == 'describe':
            source_describe = getattr(self._source, 'describe')

            def describe() -> Iterable[Metric]:
                for metric in source_describe():
                    yield self._transform_metric_family(metric)

            return describe
        raise AttributeError(name)

    def __repr__(self) -> str:
        return "RegistryView({!r}, prefix={!r}, const_labels={!r})".format(
            self._source, self._prefix, self._const_labels)

    def _transform_metric(self, metric: Metric) -> Metric:
        transformed = self._transform_metric_family(metric)
        transformed.samples = [self._transform_sample(metric, sample) for sample in metric.samples]
        return transformed

    def _transform_metric_family(self, metric: Metric) -> Metric:
        return Metric(self._prefix + metric.name, metric.documentation, metric.type, metric.unit)

    def _transform_sample(self, metric: Metric, sample: Sample) -> Sample:
        labels = sample.labels
        if self._const_labels:
            collisions = set(self._const_labels).intersection(labels)
            if collisions:
                raise ConstLabelCollisionError(
                    'Const labels {} collide with existing labels of sample {!r}'.format(
                        sorted(collisions), sample.name))
            labels = dict(labels, **self._const_labels)
        else:
            labels = dict(labels)
        return Sample(
            self._transform_sample_name(metric, sample.name),
            labels,
            sample.value,
            sample.timestamp,
            sample.exemplar,
            sample.native_histogram,
        )

    def _transform_sample_name(self, metric: Metric, sample_name: str) -> str:
        if sample_name == metric.name:
            return self._prefix + sample_name
        if sample_name.startswith(metric.name):
            suffix = sample_name[len(metric.name):]
            if suffix in _TYPE_SUFFIXES.get(metric.type, ()):
                return self._prefix + metric.name + suffix
        return self._prefix + sample_name


class _FilteringCollector:
    """Fallback for sources without restricted_registry: filters samples after collect."""

    def __init__(self, names: Iterable[str], source: Collector):
        self._names = set(names)
        self._source = source

    def collect(self) -> Iterable[Metric]:
        for metric in self._source.collect():
            restricted = metric._restricted_metric(self._names)
            if restricted:
                yield restricted
