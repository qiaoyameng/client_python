"""Streaming quantile estimation over a sliding time window.

This module implements the CKMS algorithm ("Effective Computation of Biased
Quantiles over Data Streams", Cormode, Korn, Muthukrishnan, Srivastava) to
compute approximate quantiles with a configurable rank error while keeping
memory bounded: observations are compressed into a small sample list instead
of being stored individually.

`TimeWindowQuantiles` approximates a sliding time window with a fixed number
of rotating time slices, so old observations expire without keeping them
around forever.
"""

import math
from threading import Lock
import time
from typing import Callable, Dict, Iterable, List, Tuple

# Observations are buffered and inserted into the sample list in sorted
# batches of this size, which amortizes the cost of keeping samples ordered.
_BUFFER_SIZE = 500


class Estimator:
    """A CKMS streaming quantile estimator with bounded memory.

    `invariants` is a sequence of `(quantile, error)` tuples. For each target
    quantile `q`, the value returned by `query(q)` is guaranteed to have a
    true rank within `+/- error * n` of the ideal rank `q * n`, where `n` is
    the number of observations seen by this estimator.
    """

    def __init__(self, invariants: Iterable[Tuple[float, float]]) -> None:
        self._invariants: Tuple[Tuple[float, float], ...] = tuple(invariants)
        if not self._invariants:
            raise ValueError('At least one (quantile, error) invariant is required')
        for quantile, error in self._invariants:
            if not 0.0 < quantile < 1.0:
                raise ValueError('Quantile targets must be in the open interval (0, 1), got %r' % (quantile,))
            if not 0.0 < error < 1.0:
                raise ValueError('Quantile errors must be in the open interval (0, 1), got %r' % (error,))
        self._reset()

    def _reset(self) -> None:
        # Parallel lists, kept sorted by value:
        # _values[i] has rank in [_ranks[i] - _delta[i], _ranks[i] + _delta[i]].
        self._values: List[float] = []
        self._g: List[int] = []
        self._delta: List[float] = []
        self._ranks: List[int] = []
        self._buffer: List[float] = []
        self._count = 0

    @property
    def count(self) -> int:
        return self._count

    def observe(self, value: float) -> None:
        self._buffer.append(value)
        self._count += 1
        if len(self._buffer) >= _BUFFER_SIZE:
            self._flush()

    def _invariant(self, rank: float, n: int) -> float:
        """The allowed rank error at `rank` out of `n` observations."""
        best = math.inf
        for quantile, error in self._invariants:
            if quantile * n <= rank:
                bound = error * (n - rank) / (1.0 - quantile)
            else:
                bound = error * rank / quantile
            if bound < best:
                best = bound
        return best

    def _flush(self) -> None:
        if not self._buffer:
            return
        self._buffer.sort()
        # Merge the sorted buffer into the sorted sample list in one pass.
        values: List[float] = []
        g: List[int] = []
        delta: List[float] = []
        ranks: List[int] = []
        rank = 0
        index = 0
        for value in self._buffer:
            while index < len(self._values) and self._values[index] < value:
                rank += self._g[index]
                values.append(self._values[index])
                g.append(self._g[index])
                delta.append(self._delta[index])
                ranks.append(rank)
                index += 1
            if not values or index == len(self._values):
                # Keep the exact minimum and maximum.
                new_delta = 0.0
            else:
                new_delta = math.floor(self._invariant(rank + 1, self._count))
            values.append(value)
            g.append(1)
            delta.append(new_delta)
            rank += 1
            ranks.append(rank)
        while index < len(self._values):
            rank += self._g[index]
            values.append(self._values[index])
            g.append(self._g[index])
            delta.append(self._delta[index])
            ranks.append(rank)
            index += 1
        self._values, self._g, self._delta, self._ranks = values, g, delta, ranks
        self._buffer = []
        self._compress()

    def _compress(self) -> None:
        # Merge adjacent samples whose combined rank spread stays within the
        # invariant. Scan right to left; never merge away the exact minimum
        # (index 0) or maximum (last index).
        index = len(self._values) - 2
        while index >= 1:
            if self._g[index] + self._g[index + 1] + self._delta[index + 1] <= self._invariant(self._ranks[index + 1], self._count):
                self._g[index + 1] += self._g[index]
                del self._values[index], self._g[index], self._delta[index], self._ranks[index]
            index -= 1

    def query(self, quantile: float) -> float:
        """Return the estimated value at `quantile`, or NaN if empty."""
        self._flush()
        if not self._values:
            return math.nan
        n = self._count
        target = math.ceil(quantile * n)
        target += math.ceil(self._invariant(target, n) / 2.0)
        rank = 0
        for i in range(len(self._values) - 1):
            rank += self._g[i]
            if rank + self._g[i + 1] + self._delta[i + 1] > target:
                return self._values[i]
        return self._values[-1]

    def merge_samples_into(self, other: 'Estimator') -> None:
        """Append this estimator's compressed samples to `other`.

        Merging compressed samples preserves the rank error guarantee: each
        source estimator contributes at most its own configured error, so the
        merged view errs by at most `error * total_count`.
        """
        self._flush()
        other._values.extend(self._values)
        other._g.extend(self._g)
        other._delta.extend(self._delta)
        other._count += self._count

    def _sort_after_merge(self) -> None:
        order = sorted(range(len(self._values)), key=self._values.__getitem__)
        self._values = [self._values[i] for i in order]
        self._g = [self._g[i] for i in order]
        self._delta = [self._delta[i] for i in order]
        rank = 0
        self._ranks = []
        for g in self._g:
            rank += g
            self._ranks.append(rank)


class TimeWindowQuantiles:
    """Approximate quantiles over a sliding time window.

    The window of `max_age` seconds is approximated by `age_buckets` rotating
    time slices of `max_age / age_buckets` seconds each. An observation
    remains visible for between `max_age - max_age_error` and `max_age`
    seconds, where `max_age_error` is the slice duration. Memory is bounded:
    each slice holds a compressed CKMS estimator, not raw observations.
    """

    def __init__(self,
                 invariants: Iterable[Tuple[float, float]],
                 max_age: float = 600.0,
                 age_buckets: int = 5,
                 clock: Callable[[], float] = time.time,
                 ) -> None:
        invariants = tuple(invariants)
        if not max_age > 0:
            raise ValueError('max_age must be positive, got %r' % (max_age,))
        if age_buckets < 1:
            raise ValueError('age_buckets must be at least 1, got %r' % (age_buckets,))
        self._invariants = invariants
        self._max_age = float(max_age)
        self._age_buckets = int(age_buckets)
        self._slice_duration = self._max_age / self._age_buckets
        self._clock = clock
        self._slices: List[Estimator] = [Estimator(invariants) for _ in range(self._age_buckets)]
        self._current = 0
        self._last_rotation = float(clock())
        self._lock = Lock()

    @property
    def max_age(self) -> float:
        """The nominal window size in seconds."""
        return self._max_age

    @property
    def age_buckets(self) -> int:
        """The number of time slices approximating the window."""
        return self._age_buckets

    @property
    def max_age_error(self) -> float:
        """The maximum error in the effective window size, in seconds."""
        return self._slice_duration

    def observe(self, value: float) -> None:
        with self._lock:
            self._rotate()
            self._slices[self._current].observe(value)

    def _rotate(self) -> None:
        now = float(self._clock())
        elapsed = now - self._last_rotation
        if elapsed < self._slice_duration:
            # Also covers clock jumps backwards: keep the current slices.
            return
        steps = int(elapsed // self._slice_duration)
        if steps >= self._age_buckets:
            # Every slice has expired; start over with an empty window.
            for s in self._slices:
                s._reset()
            self._current = 0
            self._last_rotation = now
            return
        for _ in range(steps):
            self._current = (self._current + 1) % self._age_buckets
            self._slices[self._current]._reset()
        self._last_rotation += steps * self._slice_duration

    def snapshot(self) -> Dict[float, float]:
        """Return a consistent snapshot of all target quantiles.

        All values are computed from the same rotation state, so concurrent
        observations cannot make different quantiles reflect different points
        in time. Quantiles are NaN when the window holds no observations.
        """
        with self._lock:
            self._rotate()
            merged = Estimator(self._invariants)
            for s in self._slices:
                s.merge_samples_into(merged)
            merged._sort_after_merge()
            return {quantile: merged.query(quantile) for quantile, _ in self._invariants}
