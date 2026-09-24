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


class MmapedDictError(Exception):
    """Base class for errors reading a mmaped dict file."""


class IncompleteMmapedFileError(MmapedDictError):
    """The file looks like it is still being initialized by its writer.

    This is a transient condition: a fresh file is created empty and its
    header is only written afterwards, so a reader racing with the writer
    may observe it. Retrying the read after a short delay may succeed.
    """


class CorruptedMmapedFileError(MmapedDictError, RuntimeError):
    """The file content is structurally invalid and will not heal by retrying.

    Subclasses RuntimeError for backwards compatibility with the error
    previously raised for corrupted files.
    """


def _pack_two_doubles(data, pos, value, timestamp):
    data[pos:pos + 16] = _pack_two_doubles_func(value, timestamp)


def _pack_integer(data, pos, value):
    data[pos:pos + 4] = _pack_integer_func(value)


def _check_used(used, available):
    """Validate the `used` header value against the size of the data read."""
    if used == 0:
        # A freshly created file is all zeros until the writer initializes it.
        raise IncompleteMmapedFileError('file header not yet initialized (used == 0)')
    if used < 8:
        raise CorruptedMmapedFileError(f'invalid used value {used}: smaller than the 8 byte header')
    if used > available:
        raise CorruptedMmapedFileError(f'used value {used} exceeds file size {available}')


def _read_all_values(data, used=0):
    """Yield (key, value, timestamp, pos). No locking is performed."""

    if used <= 0:
        # If not valid `used` value is passed in, read it from the file.
        used = _unpack_integer(data, 0)[0]

    pos = 8

    while pos < used:
        if pos + 4 > used:
            raise CorruptedMmapedFileError(f'truncated record length at offset {pos}')
        encoded_len = _unpack_integer(data, pos)[0]
        if encoded_len < 0:
            raise CorruptedMmapedFileError(f'negative record length {encoded_len} at offset {pos}')
        padded_len = encoded_len + (8 - (encoded_len + 4) % 8)
        value_pos = pos + 4 + padded_len
        if value_pos % 8 != 0:
            raise CorruptedMmapedFileError(f'misaligned record value at offset {value_pos}')
        # Check the whole record (key, padding and the two doubles) is
        # contained in the used region before reading any of it, so a
        # truncated record can never contribute a partial value.
        if value_pos + 16 > used:
            raise CorruptedMmapedFileError(f'truncated record at offset {pos}: extends beyond used {used}')
        encoded_key = data[pos + 4:pos + 4 + encoded_len]
        try:
            key = encoded_key.decode('utf-8')
        except UnicodeDecodeError as e:
            raise CorruptedMmapedFileError(f'invalid utf-8 key at offset {pos + 4}') from e
        value, timestamp = _unpack_two_doubles(data, value_pos)
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
            if len(data) < 8:
                # The file was just created and its header has not been
                # written yet; this is a transient state.
                raise IncompleteMmapedFileError(
                    f'file too short for header: {len(data)} bytes')
            used = _unpack_integer(data, 0)[0]
            if used > len(data):  # Then read in the rest, if needed.
                data += infp.read(used - len(data))
            _check_used(used, len(data))
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
