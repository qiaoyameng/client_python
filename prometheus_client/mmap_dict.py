import json
import mmap
import os
import struct
from typing import List

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


class MmapedDictError(RuntimeError):
    """Base class for errors raised while reading an mmaped dict file."""


class MmapedDictCorruptedError(MmapedDictError):
    """The file content is invalid in a way that no retry can fix."""


class MmapedDictIncompleteError(MmapedDictError):
    """The file looks like it is still being initialized by a writer.

    A freshly created or growing file can be observed in an intermediate
    state (e.g. before its header is published). Retrying the read later
    may succeed.
    """


def _read_all_values(data, used=0):
    """Yield (key, value, timestamp, pos). No locking is performed."""

    if len(data) < 8:
        raise MmapedDictIncompleteError(
            f'File header is incomplete: only {len(data)} of 8 bytes are present.')

    if used <= 0:
        # If not valid `used` value is passed in, read it from the file.
        used = _unpack_integer(data, 0)[0]

    if used == 0:
        # A freshly created file has a zeroed header until the writer
        # publishes the initial value.
        raise MmapedDictIncompleteError('File header is not yet initialized: used is 0.')
    if used < 8:
        raise MmapedDictCorruptedError(f'Invalid used value in header: {used}.')
    if used > len(data):
        # The writer grows the file before publishing `used`, so this means
        # we raced with a file that is still being extended.
        raise MmapedDictIncompleteError(
            f'File is still being extended: used is {used} but only {len(data)} bytes are present.')

    pos = 8

    while pos < used:
        if pos + 4 > used:
            raise MmapedDictCorruptedError(
                f'Truncated record at {pos}: length header extends beyond used ({used}).')
        encoded_len = _unpack_integer(data, pos)[0]
        if encoded_len < 0:
            raise MmapedDictCorruptedError(f'Negative record length {encoded_len} at {pos}.')
        # Entries are padded so that values are 8-byte aligned.
        padded_len = encoded_len + (8 - (encoded_len + 4) % 8)
        value_pos = pos + 4 + padded_len
        # Check the whole record, including padding and the two doubles, is
        # within the committed region before reading key and value.
        if value_pos + 16 > used:
            raise MmapedDictCorruptedError(
                f'Truncated record at {pos}: record of {encoded_len} bytes extends beyond used ({used}).')
        encoded_key = data[pos + 4:pos + 4 + encoded_len]
        if data[pos + 4 + encoded_len:value_pos] != b' ' * (padded_len - encoded_len):
            raise MmapedDictCorruptedError(f'Invalid record alignment padding at {pos}.')
        value, timestamp = _unpack_two_doubles(data, value_pos)
        try:
            key = encoded_key.decode('utf-8')
        except UnicodeDecodeError as e:
            raise MmapedDictCorruptedError(f'Invalid UTF-8 in record key at {pos}: {e}.')
        yield key, value, timestamp, value_pos
        pos = value_pos + 16


class MmapedDict:
    """A dict of doubles, backed by an mmapped file.

    The file starts with a 4 byte int, indicating how much of it is used.
    Then 4 bytes of padding.
    There's then a number of entries, consisting of a 4 byte int which is the
    size of the next field, a utf-8 encoded string key, padding to a 8 byte
    alignment, and then a 8 byte float which is the value and a 8 byte float
    which is a UNIX timestamp in seconds.

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
            used = 0
            if len(data) >= 4:
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
