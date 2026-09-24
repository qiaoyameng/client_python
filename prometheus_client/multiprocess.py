from collections import defaultdict
import glob
import json
import logging
import os
import time
import warnings

from .metrics import Gauge
from .metrics_core import Metric
from .mmap_dict import (
    MmapedDict, MmapedDictError, MmapedDictIncompleteError,
)
from .samples import Sample
from .utils import floatToGoString

logger = logging.getLogger(__name__)

try:  # Python3
    FileNotFoundError
except NameError:  # Python >= 2.5
    FileNotFoundError = IOError


class MultiProcessCollector:
    """Collector for files for multi-process mode."""

    def __init__(self, registry, path=None, strict=True, max_read_retries=2):
        if path is None:
            # This deprecation warning can go away in a few releases when removing the compatibility
            if 'prometheus_multiproc_dir' in os.environ and 'PROMETHEUS_MULTIPROC_DIR' not in os.environ:
                os.environ['PROMETHEUS_MULTIPROC_DIR'] = os.environ['prometheus_multiproc_dir']
                warnings.warn("prometheus_multiproc_dir variable has been deprecated in favor of the upper case naming PROMETHEUS_MULTIPROC_DIR", DeprecationWarning)
            path = os.environ.get('PROMETHEUS_MULTIPROC_DIR')
        if not path or not os.path.isdir(path):
            raise ValueError('env PROMETHEUS_MULTIPROC_DIR is not set or not a directory')
        self._path = path
        self._strict = strict
        self._max_read_retries = max_read_retries
        if registry:
            registry.register(self)

    @staticmethod
    def merge(files, accumulate=True, strict=True, max_read_retries=2):
        """Merge metrics from given mmap files.

        By default, histograms are accumulated, as per prometheus wire format.
        But if writing the merged data back to mmap files, use
        accumulate=False to avoid compound accumulation.

        In strict mode (the default) a file that cannot be parsed fails the
        whole merge. With strict=False, files that are corrupted or still
        being initialized are skipped instead, with the reason logged.
        """
        metrics = MultiProcessCollector._read_metrics(files, strict, max_read_retries)
        return MultiProcessCollector._accumulate_metrics(metrics, accumulate)

    @staticmethod
    def _read_file_values(f, strict, max_read_retries):
        """Read all values from a single mmap file.

        Files that appear to still be initializing are re-read a bounded
        number of times before giving up.
        """
        attempts = 1 if strict else max_read_retries + 1
        for attempt in range(attempts):
            try:
                # Materialize the values so that read errors are raised here,
                # within the retry loop, rather than during later iteration.
                return list(MmapedDict.read_all_values_from_file(f))
            except MmapedDictIncompleteError:
                if attempt == attempts - 1:
                    raise
                time.sleep(0.001)

    @staticmethod
    def _read_metrics(files, strict=True, max_read_retries=2):
        metrics = {}
        key_cache = {}

        def _parse_key(key):
            val = key_cache.get(key)
            if not val:
                metric_name, name, labels, help_text = json.loads(key)
                labels_key = tuple(sorted(labels.items()))
                val = key_cache[key] = (metric_name, name, labels, labels_key, help_text)
            return val

        for f in files:
            parts = os.path.basename(f).split('_')
            typ = parts[0]
            try:
                file_values = MultiProcessCollector._read_file_values(f, strict, max_read_retries)
            except FileNotFoundError:
                if typ == 'gauge' and parts[1].startswith('live'):
                    # Files for 'live*' gauges can be deleted between the glob of collect
                    # and now (via a mark_process_dead call) so don't fail if
                    # the file is missing
                    continue
                raise
            except MmapedDictError as e:
                if strict:
                    raise
                logger.warning('Skipping file %s during collection: %s', f, e)
                continue
            for key, value, timestamp, _ in file_values:
                metric_name, name, labels, labels_key, help_text = _parse_key(key)

                metric = metrics.get(metric_name)
                if metric is None:
                    metric = Metric(metric_name, help_text, typ)
                    metrics[metric_name] = metric

                if typ == 'gauge':
                    pid = parts[2][:-3]
                    metric._multiprocess_mode = parts[1]
                    metric.add_sample(name, labels_key + (('pid', pid),), value, timestamp)
                else:
                    # The duplicates and labels are fixed in the next for.
                    metric.add_sample(name, labels_key, value)
        return metrics

    @staticmethod
    def _accumulate_metrics(metrics, accumulate):
        for metric in metrics.values():
            samples = defaultdict(lambda: defaultdict(float))
            sample_timestamps = defaultdict(lambda: defaultdict(float))
            buckets = defaultdict(lambda: defaultdict(float))
            for s in metric.samples:
                name, labels, value, timestamp, exemplar, native_histogram_value = s

                if (
                    metric.type == 'gauge'
                    and metric._multiprocess_mode in (
                        'min', 'livemin',
                        'max', 'livemax',
                        'sum', 'livesum',
                        'mostrecent', 'livemostrecent',
                    )
                ):
                    labels = tuple(l for l in labels if l[0] != 'pid')

                if metric.type == 'gauge':
                    if metric._multiprocess_mode in ('min', 'livemin'):
                        current = samples[labels].setdefault((name, labels), value)
                        if value < current:
                            samples[labels][(name, labels)] = value
                    elif metric._multiprocess_mode in ('max', 'livemax'):
                        current = samples[labels].setdefault((name, labels), value)
                        if value > current:
                            samples[labels][(name, labels)] = value
                    elif metric._multiprocess_mode in ('sum', 'livesum'):
                        samples[labels][(name, labels)] += value
                    elif metric._multiprocess_mode in ('mostrecent', 'livemostrecent'):
                        current_timestamp = sample_timestamps[labels][name]
                        timestamp = float(timestamp or 0)
                        if current_timestamp < timestamp:
                            samples[labels][(name, labels)] = value
                            sample_timestamps[labels][name] = timestamp
                    else:  # all/liveall
                        samples[labels][(name, labels)] = value

                elif metric.type == 'histogram':
                    # A for loop with early exit is faster than a genexpr
                    # or a listcomp that ends up building unnecessary things
                    for l in labels:
                        if l[0] == 'le':
                            bucket_value = float(l[1])
                            # _bucket
                            without_le = tuple(l for l in labels if l[0] != 'le')
                            buckets[without_le][bucket_value] += value
                            break
                    else:  # did not find the `le` key
                        # _sum/_count
                        samples[labels][(name, labels)] += value
                else:
                    # Counter and Summary.
                    samples[labels][(name, labels)] += value

            # Accumulate bucket values.
            if metric.type == 'histogram':
                for labels, values in buckets.items():
                    acc = 0.0
                    for bucket, value in sorted(values.items()):
                        sample_key = (
                            metric.name + '_bucket',
                            labels + (('le', floatToGoString(bucket)),),
                        )
                        if accumulate:
                            acc += value
                            samples[labels][sample_key] = acc
                        else:
                            samples[labels][sample_key] = value
                    if accumulate:
                        samples[labels][(metric.name + '_count', labels)] = acc

            # Convert to correct sample format.
            metric.samples = []
            for _, samples_by_labels in samples.items():
                for (name_, labels), value in samples_by_labels.items():
                    metric.samples.append(Sample(name_, dict(labels), value))
        return metrics.values()

    def collect(self):
        files = glob.glob(os.path.join(self._path, '*.db'))
        return self.merge(files, accumulate=True, strict=self._strict,
                          max_read_retries=self._max_read_retries)


_LIVE_GAUGE_MULTIPROCESS_MODES = {m for m in Gauge._MULTIPROC_MODES if m.startswith('live')}


def mark_process_dead(pid, path=None):
    """Do bookkeeping for when one process dies in a multi-process setup."""
    if path is None:
        path = os.environ.get('PROMETHEUS_MULTIPROC_DIR', os.environ.get('prometheus_multiproc_dir'))
    for mode in _LIVE_GAUGE_MULTIPROCESS_MODES:
        for f in glob.glob(os.path.join(path, f'gauge_{mode}_{pid}.db')):
            os.remove(f)
