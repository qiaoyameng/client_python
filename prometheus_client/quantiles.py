from __future__ import annotations

import math
from collections import deque
from threading import RLock
from typing import (
    Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple,
)


def _finite_float(value: float, name: str = 'value') -> float:
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f'{name} must be finite.')
    return value


class TDigestSketch:
    """Mergeable rank summary that remains exact for small streams."""

    def __init__(self, rank_error: float,
                 values: Optional[Sequence[float]] = None) -> None:
        if not 0.0 < rank_error < 1.0:
            raise ValueError('rank_error must be between 0 and 1.')
        self.rank_error = rank_error
        self.exact_limit = math.ceil(5.0 / rank_error)
        self.count = 0
        self.exact: List[float] = []
        self.centroids: List[List[float]] = []

        for value in values or ():
            self.add(value)

    def add(self, value: float, weight: int = 1) -> None:
        value = _finite_float(value)
        if weight <= 0:
            raise ValueError('weight must be positive.')
        previous_count = self.count
        self.count += weight
        if previous_count < self.exact_limit:
            self.exact.extend([value] * weight)
            if self.count > self.exact_limit:
                for exact_value in self.exact:
                    self._add_centroid(exact_value, 1)
                self.exact = []
            return
        self._add_centroid(value, weight)
        self._compact()

    def _scale(self, quantile: float) -> float:
        compression = 10.0 / self.rank_error
        return max(3.0, compression * quantile * (1.0 - quantile) + 1.0)

    def _add_centroid(self, value: float, weight: int) -> None:
        index = 0
        while index < len(self.centroids) and self.centroids[index][0] < value:
            index += 1
        if (index < len(self.centroids)
                and self.centroids[index][0] == value):
            self.centroids[index][1] += weight
        else:
            self.centroids.insert(index, [value, float(weight)])

    def _compact(self) -> None:
        if len(self.centroids) < 3:
            return
        compacted: List[List[float]] = [self.centroids[0]]
        cumulative = self.centroids[0][1]
        for value, weight in self.centroids[1:-1]:
            quantile = cumulative / self.count
            if compacted[-1][1] + weight < self._scale(quantile):
                old_value, old_weight = compacted[-1]
                compacted[-1][0] = (
                    (old_value * old_weight + value * weight)
                    / (old_weight + weight))
                compacted[-1][1] += weight
            else:
                compacted.append([value, weight])
            cumulative += weight
        compacted.append(self.centroids[-1])
        self.centroids = compacted

    @classmethod
    def merge(cls, rank_error: float,
              sketches: Sequence['TDigestSketch']) -> 'TDigestSketch':
        merged = cls(rank_error)
        merged.count = sum(sketch.count for sketch in sketches)
        merged.exact = []
        merged.centroids = []
        for sketch in sketches:
            merged.exact.extend(sketch.exact)
            for value, weight in sketch.centroids:
                merged._add_centroid(value, int(weight))
        if len(merged.exact) <= merged.exact_limit and not merged.centroids:
            merged.count = len(merged.exact)
            return merged
        for value in merged.exact:
            merged._add_centroid(value, 1)
        merged.exact = []
        if merged.count > merged.exact_limit:
            merged._compact()
        return merged

    def quantile(self, quantile: float) -> float:
        if not 0.0 <= quantile <= 1.0:
            raise ValueError('quantile must be between 0 and 1.')
        if self.count == 0:
            return math.nan
        if self.exact:
            return sorted(self.exact)[math.ceil(quantile * self.count) - 1]

        target = quantile * self.count
        cumulative = 0.0
        for value, weight in self.centroids:
            if cumulative + weight >= target:
                return value
            cumulative += weight
        return self.centroids[-1][0]


class TimeWindowQuantiles:
    def __init__(self, quantile_errors: Mapping[float, float],
                 max_age_seconds: float, time_slices: int,
                 clock: Callable[[], float]) -> None:
        max_age_seconds = _finite_float(max_age_seconds, 'max_age_seconds')
        if max_age_seconds <= 0:
            raise ValueError('max_age_seconds must be positive.')
        if not isinstance(time_slices, int) or time_slices <= 0:
            raise ValueError('time_slices must be a positive integer.')

        self.quantile_errors = {
            float(quantile): float(rank_error)
            for quantile, rank_error in quantile_errors.items()}
        self.quantiles = tuple(sorted(self.quantile_errors))
        for quantile in self.quantiles:
            if not 0.0 <= quantile <= 1.0:
                raise ValueError('quantiles must be between 0 and 1.')
        for rank_error in self.quantile_errors.values():
            if not 0.0 < rank_error < 1.0:
                raise ValueError('rank_error must be between 0 and 1.')

        self.max_age_seconds = max_age_seconds
        self.time_slices = time_slices
        self.time = clock
        self.slice_width_seconds = max_age_seconds / time_slices
        self.max_expiration_error_seconds = self.slice_width_seconds
        self.effective_window_seconds = (
            max_age_seconds - self.slice_width_seconds, max_age_seconds)

        self._lock = RLock()
        self._buckets: Deque[
            Tuple[float, Dict[float, TDigestSketch]]] = deque()
        self._latest_clock: Optional[float] = None

    def _check_clock(self, now: float) -> float:
        now = _finite_float(now, 'clock value')
        if self._latest_clock is not None and now < self._latest_clock:
            raise ValueError('clock must not move backwards.')
        self._latest_clock = now
        return now

    def _rotate(self, now: float) -> None:
        width = self.slice_width_seconds
        current_start = math.floor(now / width) * width
        while self._buckets and self._buckets[0][0] + self.max_age_seconds <= now:
            self._buckets.popleft()
        if not self._buckets or self._buckets[-1][0] < current_start:
            self._buckets.append(
                (current_start, {
                    quantile: TDigestSketch(rank_error)
                    for quantile, rank_error in self.quantile_errors.items()
                }))

    def observe(self, value: float) -> None:
        value = _finite_float(value)
        with self._lock:
            now = self._check_clock(self.time())
            self._rotate(now)
            for sketch in self._buckets[-1][1].values():
                sketch.add(value)

    def snapshot(self) -> Tuple[Tuple[float, ...], int]:
        with self._lock:
            now = self._check_clock(self.time())
            self._rotate(now)
            if not self._buckets:
                return tuple(math.nan for _ in self.quantiles), 0
            values = []
            for quantile in self.quantiles:
                rank_error = self.quantile_errors[quantile]
                sketch = TDigestSketch.merge(
                    rank_error,
                    tuple(bucket[1][quantile] for bucket in self._buckets),
                )
                values.append(sketch.quantile(quantile))
            return tuple(values), len(self._buckets) and sum(
                next(iter(bucket[1].values())).count
                for bucket in self._buckets)
