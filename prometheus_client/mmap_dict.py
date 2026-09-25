import json
import mmap
import os
import struct
from typing import List

_INITIAL_MMAP_SIZE = 1 << 16
_pack_integer_func = struct.Struct(b'i').pack
_pack_unsigned_func = struct.Struct(b'I').pack
_pack_two_doubles_func = struct.Struct(b'dd').pack
_unpack_integer = struct.Struct(b'i').unpack_from
_unpack_unsigned = struct.Struct(b'I').unpack_from
_unpack_two_doubles = struct.Struct(b'dd').unpack_from

# Offset of the deletion generation counter in the file header. Files written
# by older versions have zero padding here, which reads as generation 0.
_GENERATION_OFFSET = 4


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
        # A negative length marks a tombstoned (deleted) entry. The entry is
        # left in place so that positions of other entries never move, but it
        # is skipped by readers.
        tombstoned = encoded_len < 0
        encoded_len = abs(encoded_len)
        # check we are not reading beyond bounds
        if encoded_len + pos > used:
            raise RuntimeError('Read beyond file size detected, file is corrupted.')
        pos += 4
        encoded_key = data[pos:pos + encoded_len]
        padded_len = encoded_len + (8 - (encoded_len + 4) % 8)
        pos += padded_len
        value, timestamp = _unpack_two_doubles(data, pos)
        if not tombstoned:
            yield encoded_key.decode('utf-8'), value, timestamp, pos
        pos += 16


class MmapedDict:
    """A dict of doubles, backed by an mmapped file.

    The file starts with a 4 byte int, indicating how much of it is used.
    Then a 4 byte unsigned int deletion generation (0 in files written by
    older versions of this library).
    There's then a number of entries, consisting of a 4 byte int which is the
    size of the next field (negative if the entry is a tombstone), a utf-8
    encoded string key, padding to a 8 byte alignment, and then a 8 byte float
    which is the value and a 8 byte float which is a UNIX timestamp in seconds.

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
        self._generation = self.read_generation()
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

    def read_value_if_present(self, key):
        """Like read_value, but returns None instead of initializing a missing key."""
        if key not in self._positions:
            return None
        return _unpack_two_doubles(self._m, self._positions[key])

    def write_value(self, key, value, timestamp):
        if key not in self._positions:
            self._init_value(key)
        pos = self._positions[key]
        _pack_two_doubles(self._m, pos, value, timestamp)

    def read_generation(self):
        """Read the current deletion generation from the file header."""
        return _unpack_unsigned(self._m, _GENERATION_OFFSET)[0]

    def bump_generation(self):
        """Increment the deletion generation. No locking is performed."""
        self._generation += 1
        self._m[_GENERATION_OFFSET:_GENERATION_OFFSET + 4] = _pack_unsigned_func(self._generation)

    def sync(self):
        """Re-scan the file if its deletion generation changed on disk.

        Returns True if the file was re-scanned. No locking is performed."""
        generation = self.read_generation()
        if generation == self._generation:
            return False
        self._generation = generation
        self._used = _unpack_integer(self._m, 0)[0]
        self._positions = {}
        for key, _, _, pos in self._read_all_values():
            self._positions[key] = pos
        return True

    def delete_matching(self, predicate):
        """Tombstone all entries whose decoded key satisfies predicate(key).

        Bumps the deletion generation if anything was deleted and returns the
        number of entries tombstoned. No locking is performed."""
        deleted = 0
        pos = 8
        while pos < self._used:
            encoded_len = _unpack_integer(self._m, pos)[0]
            tombstoned = encoded_len < 0
            encoded_len = abs(encoded_len)
            if encoded_len + pos > self._used:
                raise RuntimeError('Read beyond file size detected, file is corrupted.')
            key = None
            if not tombstoned:
                key = self._m[pos + 4:pos + 4 + encoded_len].decode('utf-8')
            padded_len = encoded_len + (8 - (encoded_len + 4) % 8)
            if key is not None and predicate(key):
                _pack_integer(self._m, pos, -encoded_len)
                self._positions.pop(key, None)
                deleted += 1
            pos += 4 + padded_len + 16
        if deleted:
            self.bump_generation()
        return deleted

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
