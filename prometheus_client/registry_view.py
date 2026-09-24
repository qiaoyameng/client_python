from typing import Dict, Iterable, List, Optional, Set

from .metrics_core import Metric
from .registry import Collector
from .samples import Sample
from .validation import _validate_labelname


class ConstLabelCollisionError(ValueError):
    """A const label of a RegistryView collides with an existing sample label."""


class DuplicateMetricError(ValueError):
    """A RegistryView transformation produced two metric families with the same name."""


class RegistryView:
    """A read-only, transforming view over a collector or registry.

    Wraps any object with a no-argument ``collect`` method (e.g. a
    ``CollectorRegistry``, another ``RegistryView``, or a plain custom
    collector) and exposes the same metric families with a name prefix
    and a set of const labels applied. This allows a gateway to unify
    several component registries, each behind its own view, into a
    single exposition.

    The wrapped registry/collector is never modified, and the Metric
    and Sample objects returned by the underlying collectors are never
    mutated: every ``collect``/``describe`` call builds fresh Metric
    and Sample objects. Exemplars and native histograms are immutable
    values and are passed through unchanged.

    Errors are raised eagerly during collection (never silently
    overwritten) when:

    - a const label collides with a label already present on a sample
      (``ConstLabelCollisionError``), or
    - the transformation yields two metric families with the same name
      (``DuplicateMetricError``).
    """

    def __init__(self, source: Collector, prefix: str = '', const_labels: Optional[Dict[str, str]] = None):
        self._source = source
        self._prefix = prefix
        self._const_labels: Dict[str, str] = dict(const_labels) if const_labels else {}
        for label_name in self._const_labels:
            _validate_labelname(label_name)

    @property
    def prefix(self) -> str:
        return self._prefix

    @property
    def const_labels(self) -> Dict[str, str]:
        return dict(self._const_labels)

    def _transform_metric(self, metric: Metric) -> Metric:
        transformed = Metric(
            self._prefix + metric.name,
            metric.documentation,
            metric.type,
            metric.unit,
        )
        const_labels = self._const_labels
        samples: List[Sample] = []
        for sample in metric.samples:
            labels = dict(sample.labels)
            if const_labels:
                collisions = const_labels.keys() & labels.keys()
                if collisions:
                    raise ConstLabelCollisionError(
                        'Const label(s) {} of RegistryView collide with existing labels on sample {!r} '
                        'of metric {!r}'.format(sorted(collisions), sample.name, metric.name))
                labels.update(const_labels)
            samples.append(Sample(
                self._prefix + sample.name,
                labels,
                sample.value,
                sample.timestamp,
                sample.exemplar,
                sample.native_histogram,
            ))
        transformed.samples = samples
        return transformed

    def _transform(self, metrics: Iterable[Metric]) -> Iterable[Metric]:
        seen: Set[str] = set()
        for metric in metrics:
            transformed = self._transform_metric(metric)
            if transformed.name in seen:
                raise DuplicateMetricError(
                    'RegistryView transformation produced a duplicate metric family name: {!r}'.format(
                        transformed.name))
            seen.add(transformed.name)
            yield transformed

    def collect(self) -> Iterable[Metric]:
        """Yields the transformed metrics from the wrapped source."""
        yield from self._transform(self._source.collect())

    def describe(self) -> Iterable[Metric]:
        """Yields the transformed metric metadata, consistent with collect.

        Uses the source's ``describe`` if it has one, otherwise falls
        back to the source's ``collect`` (as auto-describe registries do),
        so the names seen here always match what ``collect`` produces.
        """
        describe = getattr(self._source, 'describe', None)
        if describe is not None:
            yield from self._transform(describe())
        else:
            yield from self._transform(self._source.collect())

    def _source_names(self, names: Iterable[str]) -> Set[str]:
        """Maps view-level sample names back to source-level sample names."""
        return {name[len(self._prefix):] for name in names if name.startswith(self._prefix)}

    def restricted_registry(self, names: Iterable[str]) -> "RestrictedRegistryView":
        """Returns a view that only collects the given (view-level) sample names.

        The names are mapped back to the source's names, so a restricted
        collection only pulls the matching metrics from the underlying
        registry.

        Intended usage is:
            generate_latest(view.restricted_registry(['prefix_a_timeseries']))

        Experimental."""
        return RestrictedRegistryView(set(names), self)

    def get_sample_value(self, name: str, labels: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Returns the sample value, or None if not found.

        This is inefficient, and intended only for use in unittests.
        """
        if labels is None:
            labels = {}
        for metric in self.collect():
            for s in metric.samples:
                if s.name == name and s.labels == labels:
                    return s.value
        return None


class RestrictedRegistryView:
    def __init__(self, names: Set[str], view: RegistryView):
        self._name_set = names
        self._view = view

    def collect(self) -> Iterable[Metric]:
        view = self._view
        source_names = view._source_names(self._name_set)
        restricted = getattr(view._source, 'restricted_registry', None)
        if restricted is not None:
            yield from view._transform(restricted(source_names).collect())
            return
        for metric in view.collect():
            m = metric._restricted_metric(self._name_set)
            if m:
                yield m
