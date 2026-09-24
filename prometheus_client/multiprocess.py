from collections import defaultdict, namedtuple
import glob
import json
import os
import time
import warnings

from .metrics import Gauge
from .metrics_core import Metric
from .mmap_dict import (
    CorruptedMmapedFileError, IncompleteMmapedFileError, MmapedDict,
)
from .samples import Sample
from .utils import floatToGoString

try:  # Python3
    FileNotFoundError
except NameError:  # Python >= 2.5
    FileNotFoundError = IOError


#: Describes a file that was skipped during a tolerant collect, with the
#: reason why it could not be read.
SkippedFile = namedtuple('SkippedFile', ['path', 'reason'])


class MultiProcessCollector:
    """Collector for files for multi-process mode."""

    def __init__(self, registry, path=None, tolerant=False, max_read_retries=3, retry_delay=0.005):
        """If tolerant is True, files that cannot be read (because they are
        still being initialized by a writer, or because they are corrupted)
        are skipped instead of failing the whole collection. Incompletely
        initialized files are re-read at most max_read_retries times, with
        retry_delay seconds between attempts, before being skipped. The
        skipped files of the most recent collection are available, with the
        reasons, on the `skipped` attribute.

        The default tolerant=False keeps the strict behavior of failing the
        collection on any unreadable file.
        """
        if path is None:
            # This deprecation warning can go away in a few releases when removing the compatibility
            if 'prometheus_multiproc_dir' in os.environ and 'PROMETHEUS_MULTIPROC_DIR' not in os.environ:
                os.environ['PROMETHEUS_MULTIPROC_DIR'] = os.environ['prometheus_multiproc_dir']
                warnings.warn("prometheus_multiproc_dir variable has been deprecated in favor of the upper case naming PROMETHEUS_MULTIPROC_DIR", DeprecationWarning)
            path = os.environ.get('PROMETHEUS_MULTIPROC_DIR')
        if not path or not os.path.isdir(path):
            raise ValueError('env PROMETHEUS_MULTIPROC_DIR is not set or not a directory')
        self._path = path
        self._tolerant = tolerant
        self._max_read_retries = max_read_retries
        self._retry_delay = retry_delay
        self.skipped = []
        if registry:
            registry.register(self)

    @staticmethod
    def merge(files, accumulate=True, tolerant=False, max_read_retries=3, retry_delay=0.005, on_skip=None):
        """Merge metrics from given mmap files.

        By default, histograms are accumulated, as per prometheus wire format.
        But if writing the merged data back to mmap files, use
        accumulate=False to avoid compound accumulation.

        If tolerant is True, files that are still being initialized are
        re-read up to max_read_retries times and then skipped, and corrupted
        files are skipped; each skipped file is reported to the on_skip
        callback as on_skip(path, reason). If tolerant is False (the
        default), any unreadable file fails the merge.
        """
        metrics = MultiProcessCollector._read_metrics(files, tolerant, max_read_retries, retry_delay, on_skip)
        return MultiProcessCollector._accumulate_metrics(metrics, accumulate)

    @staticmethod
    def _read_file_values(f, tolerant, max_read_retries, retry_delay):
        if not tolerant:
            return list(MmapedDict.read_all_values_from_file(f))
        attempts = max_read_retries + 1
        for attempt in range(attempts):
            try:
                # Materialize the generator so that the whole file is
                # validated before any of its values are used: a file
                # either contributes all of its records or none of them.
                return list(MmapedDict.read_all_values_from_file(f))
            except IncompleteMmapedFileError:
                if attempt == attempts - 1:
                    raise
                time.sleep(retry_delay)

    @staticmethod
    def _read_metrics(files, tolerant=False, max_read_retries=3, retry_delay=0.005, on_skip=None):
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
                file_values = MultiProcessCollector._read_file_values(
                    f, tolerant, max_read_retries, retry_delay)
            except FileNotFoundError:
                if typ == 'gauge' and parts[1].startswith('live'):
                    # Files for 'live*' gauges can be deleted between the glob of collect
                    # and now (via a mark_process_dead call) so don't fail if
                    # the file is missing
                    continue
                raise
            except (IncompleteMmapedFileError, CorruptedMmapedFileError) as e:
                if not tolerant:
                    raise
                if on_skip is not None:
                    on_skip(f, str(e))
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
        skipped = []
        metrics = self.merge(
            files, accumulate=True,
            tolerant=self._tolerant,
            max_read_retries=self._max_read_retries,
            retry_delay=self._retry_delay,
            on_skip=lambda path, reason: skipped.append(SkippedFile(path, reason)),
        )
        self.skipped = skipped
        return metrics


_LIVE_GAUGE_MULTIPROCESS_MODES = {m for m in Gauge._MULTIPROC_MODES if m.startswith('live')}


def mark_process_dead(pid, path=None):
    """Do bookkeeping for when one process dies in a multi-process setup."""
    if path is None:
        path = os.environ.get('PROMETHEUS_MULTIPROC_DIR', os.environ.get('prometheus_multiproc_dir'))
    for mode in _LIVE_GAUGE_MULTIPROCESS_MODES:
        for f in glob.glob(os.path.join(path, f'gauge_{mode}_{pid}.db')):
            os.remove(f)
