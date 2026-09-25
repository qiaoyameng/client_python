import glob
import json
import mmap
import os
import struct
from typing import Callable, Dict, List, Set, Tuple

try:
    import fcntl
except ImportError:  # Windows
    fcntl = None

_INITIAL_MMAP_SIZE = 1 << 16
_pack_integer_func = struct.Struct(b'i').pack
_pack_two_doubles_func = struct.Struct(b'dd').pack
_unpack_integer = struct.Struct(b'i').unpack_from
_unpack_two_doubles = struct.Struct(b'dd').unpack_from


# struct.pack_into has atomicity issues because it will temporarily write 0 into
# the mmap, resulting in false reads to 0 when experiencing a lot of writes.
# Using direct assignment solves this issue.


def _pack_two_doubles(data, pos, value, timestamp):
    data[pos:pos + 16] = _pack_two_doubles_func(value, timestamp)


def _pack_integer(data, pos, value):
    data[pos:pos + 4] = _pack_integer_func(value)


def _read_all_values(data, used=0):
    """Yield (key, value, timestamp, pos). No locking is performed."""

    if used <= 0:
        # If not valid `used` value is passed in, read it from the file.
        used = _unpack_integer(data, 0)[0]

    pos = 8

    while pos < used:
        encoded_len = _unpack_integer(data, pos)[0]
        if encoded_len < 0:
            # A negative length marks a tombstoned (deleted) entry. Skip it,
            # using the absolute value to find the next entry.
            encoded_len = -encoded_len
            padded_len = encoded_len + (8 - (encoded_len + 4) % 8)
            if pos + 4 + padded_len + 16 > used:
                raise RuntimeError('Read beyond file size detected, file is corrupted.')
            pos += 4 + padded_len + 16
            continue
        # check we are not reading beyond bounds
        if encoded_len + pos > used:
            raise RuntimeError('Read beyond file size detected, file is corrupted.')
        pos += 4
        encoded_key = data[pos:pos + encoded_len]
        padded_len = encoded_len + (8 - (encoded_len + 4) % 8)
        pos += padded_len
        value, timestamp = _unpack_two_doubles(data, pos)
        yield encoded_key.decode('utf-8'), value, timestamp, pos
        pos += 16


class MmapedDict:
    """A dict of doubles, backed by an mmapped file.

    The file starts with a 4 byte int, indicating how much of it is used.
    Then 4 bytes of padding.
    There's then a number of entries, consisting of a 4 byte int which is the
    size of the next field, a utf-8 encoded string key, padding to a 8 byte
    alignment, and then a 8 byte float which is the value and a 8 byte float
    which is a UNIX timestamp in seconds.

    An entry whose key size is negative is a tombstone: it has been deleted
    by a remove/clear operation and must be skipped by readers. Tombstoned
    entries are never rewritten or reclaimed; the space is only recovered
    when the file is deleted (e.g. when the owning process dies and the
    directory is wiped).

    Not thread safe.
    """

    def __init__(self, filename, read_mode=False):
        self._f = open(filename, 'rb' if read_mode else 'a+b')
        self._fname = filename
        capacity = os.fstat(self._f.fileno()).st_size
        if capacity == 0:
            self._f.truncate(_INITIAL_MMAP_SIZE)
            capacity = _INITIAL_MMAP_SIZE
        self._capacity = capacity
        self._m = mmap.mmap(self._f.fileno(), self._capacity,
                            access=mmap.ACCESS_READ if read_mode else mmap.ACCESS_WRITE)

        self._positions = {}
        self._used = _unpack_integer(self._m, 0)[0]
        if self._used == 0:
            self._used = 8
            _pack_integer(self._m, 0, self._used)
        else:
            if not read_mode:
                for key, _, _, pos in self._read_all_values():
                    self._positions[key] = pos

    @staticmethod
    def read_all_values_from_file(filename):
        with open(filename, 'rb') as infp:
            # Read the first block of data, including the first 4 bytes which tell us
            # how much of the file (which is preallocated to _INITIAL_MMAP_SIZE bytes) is occupied.
            data = infp.read(mmap.PAGESIZE)
            used = _unpack_integer(data, 0)[0]
            if used > len(data):  # Then read in the rest, if needed.
                data += infp.read(used - len(data))
        return _read_all_values(data, used)

    def _init_value(self, key):
        """Initialize a value. Lock must be held by caller."""
        encoded = key.encode('utf-8')
        # Pad to be 8-byte aligned.
        padded = encoded + (b' ' * (8 - (len(encoded) + 4) % 8))
        value = struct.pack(f'i{len(padded)}sdd'.encode(), len(encoded), padded, 0.0, 0.0)
        while self._used + len(value) > self._capacity:
            self._capacity *= 2
            self._f.truncate(self._capacity)
            self._m = mmap.mmap(self._f.fileno(), self._capacity)
        self._m[self._used:self._used + len(value)] = value

        # Update how much space we've used.
        self._used += len(value)
        _pack_integer(self._m, 0, self._used)
        self._positions[key] = self._used - 16

    def _read_all_values(self):
        """Yield (key, value, pos). No locking is performed."""
        return _read_all_values(data=self._m, used=self._used)

    def read_all_values(self):
        """Yield (key, value, timestamp). No locking is performed."""
        for k, v, ts, _ in self._read_all_values():
            yield k, v, ts

    def read_value(self, key):
        if key not in self._positions:
            self._init_value(key)
        pos = self._positions[key]
        return _unpack_two_doubles(self._m, pos)

    def write_value(self, key, value, timestamp):
        if key not in self._positions:
            self._init_value(key)
        pos = self._positions[key]
        _pack_two_doubles(self._m, pos, value, timestamp)

    def close(self):
        if self._f:
            self._m.close()
            self._m = None
            self._f.close()
            self._f = None


def mmap_key(metric_name: str, name: str, labelnames: List[str], labelvalues: List[str], help_text: str) -> str:
    """Format a key for use in the mmap file."""
    # ensure labels are in consistent order for identity
    labels = dict(zip(labelnames, labelvalues))
    return json.dumps([metric_name, name, labels, help_text], sort_keys=True)


def mmap_key_with_generation(metric_name: str, name: str, labelnames: List[str], labelvalues: List[str],
                             help_text: str, generation: int) -> str:
    """Format a key for use in the mmap file, tagged with a deletion generation.

    Generation 0 produces the legacy 4-element key so that the on-disk format
    is unchanged for series that were never removed. Higher generations add a
    fifth element, which only newer clients can parse.
    """
    labels = dict(zip(labelnames, labelvalues))
    if generation:
        return json.dumps([metric_name, name, labels, help_text, generation], sort_keys=True)
    return json.dumps([metric_name, name, labels, help_text], sort_keys=True)


def parse_mmap_key(key: str) -> Tuple[str, str, Dict[str, str], str, int]:
    """Parse a key from the mmap file into (metric_name, name, labels, help_text, generation).

    Keys written by older clients have no generation element and are
    treated as generation 0.
    """
    parts = json.loads(key)
    if len(parts) == 4:
        metric_name, name, labels, help_text = parts
        return metric_name, name, labels, help_text, 0
    if len(parts) == 5:
        metric_name, name, labels, help_text, generation = parts
        return metric_name, name, labels, help_text, int(generation)
    raise ValueError('Invalid mmap key: %r' % key)


def mmap_series_key(metric_name: str, labels: Dict[str, str], histogram: bool = False) -> str:
    """Format the identity of a series for the deletion generation store.

    All samples of a series (e.g. a counter's _total, or a histogram's
    buckets, _sum and _count) share one identity, so the internal 'le'
    bucket label of histograms is excluded.
    """
    if histogram:
        labels = {k: v for k, v in labels.items() if k != 'le'}
    return json.dumps([metric_name, labels], sort_keys=True)


class GenerationStore:
    """Tracks the deletion generation of removed series in a multiprocess directory.

    Each series identity (see `mmap_series_key`) maps to a monotonically
    increasing generation counter. `remove`/`clear` increment it; writers tag
    new entries with the current generation and collectors ignore entries
    whose generation is stale. This ensures a process holding a reference to
    a deleted child cannot resurrect the old series values, and that a series
    re-created under the same name and labels starts from scratch.

    The generations are stored in `generations.json`, replaced atomically and
    guarded by an flock on `generations.lock` (a no-op on platforms without
    fcntl). Neither file matches the `*.db` glob used for metric files.
    """

    def __init__(self, directory: str):
        self._path = os.path.join(directory, 'generations.json')
        self._lock_path = os.path.join(directory, 'generations.lock')

    class _Locked:
        def __init__(self, lock_path):
            self._lock_path = lock_path

        def __enter__(self):
            self._f = open(self._lock_path, 'a+b')
            if fcntl is not None:
                fcntl.flock(self._f.fileno(), fcntl.LOCK_EX)
            return self

        def __exit__(self, *exc):
            try:
                if fcntl is not None:
                    fcntl.flock(self._f.fileno(), fcntl.LOCK_UN)
            finally:
                self._f.close()

    def locked(self) -> 'GenerationStore._Locked':
        """Exclusive cross-process lock guarding read-modify-write cycles."""
        return GenerationStore._Locked(self._lock_path)

    def _read(self) -> Dict[str, int]:
        try:
            with open(self._path, encoding='utf-8') as f:
                return {k: int(v) for k, v in json.load(f).items()}
        except FileNotFoundError:
            return {}

    def _write(self, generations: Dict[str, int]) -> None:
        # Atomic replace so concurrent readers never see a partial file.
        tmp_path = '{}.tmp.{}'.format(self._path, os.getpid())
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(generations, f, sort_keys=True)
        os.replace(tmp_path, self._path)

    def get_locked(self, series_key: str) -> int:
        """Current generation of a series. Caller must hold `locked()`."""
        return self._read().get(series_key, 0)

    def read_all(self) -> Dict[str, int]:
        """Consistent snapshot of all generations, safe to call lock-free."""
        return self._read()


def _file_is_histogram(filename: str) -> bool:
    return os.path.basename(filename).split('_')[0] == 'histogram'


def _find_series_entries(filename: str, metric_name: str,
                         labels_match: Callable[[Dict[str, str]], bool]) -> Tuple[Set[str], List[int]]:
    """Find live entries of a file belonging to matching series.

    Returns (series_keys, positions) where positions point at the 4 byte
    key-length field of each matching entry.
    """
    histogram = _file_is_histogram(filename)
    with open(filename, 'rb') as f:
        header = f.read(8)
        if len(header) < 8:
            return set(), []
        used = _unpack_integer(header, 0)[0]
        data = header + f.read(max(0, used - 8))

    series_keys = set()
    positions = []
    pos = 8
    while pos < used:
        encoded_len = _unpack_integer(data, pos)[0]
        tombstoned = encoded_len < 0
        length = -encoded_len if tombstoned else encoded_len
        padded_len = length + (8 - (length + 4) % 8)
        if pos + 4 + padded_len + 16 > used:
            raise RuntimeError('Read beyond file size detected, file is corrupted.')
        if not tombstoned:
            key = data[pos + 4:pos + 4 + length].decode('utf-8')
            key_metric_name, _, labels, _, _ = parse_mmap_key(key)
            if key_metric_name == metric_name and labels_match(labels):
                series_keys.add(mmap_series_key(metric_name, labels, histogram))
                positions.append(pos)
        pos += 4 + padded_len + 16
    return series_keys, positions


def _tombstone_entries(filename: str, positions: List[int]) -> None:
    """Mark the entries at the given key-length positions as deleted."""
    if not positions:
        return
    with open(filename, 'r+b') as f:
        for pos in positions:
            f.seek(pos)
            raw = f.read(4)
            if len(raw) < 4:
                continue
            length = _unpack_integer(raw, 0)[0]
            if length > 0:
                f.seek(pos)
                f.write(_pack_integer_func(-length))


def delete_series(directory: str, metric_name: str,
                  labels_match: Callable[[Dict[str, str]], bool]) -> List[str]:
    """Delete all series of `metric_name` whose labels satisfy `labels_match`.

    Covers every data file in `directory`, including files of other and dead
    processes. The deletion generation of each affected series is incremented
    first, then its entries are tombstoned in place. Processes still holding
    a deleted child keep writing into their old (now ignored) entries, so
    stale values are never resurrected; a series re-created afterwards starts
    from zero under the new generation.

    Returns the identities of the removed series.
    """
    store = GenerationStore(directory)
    with store.locked():
        generations = store._read()
        affected = set()
        tombstones = []
        for filename in glob.glob(os.path.join(directory, '*.db')):
            try:
                series_keys, positions = _find_series_entries(filename, metric_name, labels_match)
            except FileNotFoundError:
                # The file was removed concurrently (e.g. mark_process_dead).
                continue
            affected.update(series_keys)
            tombstones.append((filename, positions))
        for series_key in affected:
            generations[series_key] = generations.get(series_key, 0) + 1
        if affected:
            store._write(generations)
        for filename, positions in tombstones:
            try:
                _tombstone_entries(filename, positions)
            except FileNotFoundError:
                continue
    return sorted(affected)
