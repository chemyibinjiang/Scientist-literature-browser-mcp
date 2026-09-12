#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable


GIB = 1024**3


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


def _bounded_float(
    name: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


@dataclass(frozen=True)
class MemorySnapshot:
    total_bytes: int
    available_bytes: int

    @property
    def available_ratio(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return max(0.0, min(self.available_bytes / self.total_bytes, 1.0))


def parse_meminfo(text: str) -> MemorySnapshot:
    values: dict[str, int] = {}
    for raw_line in text.splitlines():
        key, separator, remainder = raw_line.partition(":")
        if not separator:
            continue
        fields = remainder.strip().split()
        if not fields:
            continue
        try:
            value = int(fields[0])
        except ValueError:
            continue
        multiplier = 1024 if len(fields) > 1 and fields[1].lower() == "kb" else 1
        values[key] = value * multiplier
    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", values.get("MemFree", 0))
    if total <= 0 or available < 0:
        raise ValueError("meminfo does not contain usable memory totals")
    return MemorySnapshot(total_bytes=total, available_bytes=min(available, total))


def read_memory_snapshot(path: Path = Path("/proc/meminfo")) -> MemorySnapshot:
    return parse_meminfo(path.read_text(encoding="utf-8"))


@dataclass(frozen=True)
class MemoryCapacityPolicy:
    maximum: int
    normal_count: int
    guarded_count: int
    constrained_count: int
    critical_count: int
    guarded_available_bytes: int
    constrained_available_bytes: int
    critical_available_bytes: int
    guarded_available_ratio: float
    constrained_available_ratio: float
    critical_available_ratio: float

    @classmethod
    def from_environ(cls, maximum: int) -> "MemoryCapacityPolicy":
        maximum = max(1, int(maximum))
        normal = _bounded_int(
            "LITERATURE_MEMORY_NORMAL_BROWSER_COUNT",
            maximum,
            1,
            maximum,
        )
        guarded_default = max(1, (normal * 4 + 4) // 5)
        constrained_default = max(1, (normal + 1) // 2)
        critical_default = max(1, (normal * 3 + 19) // 20)
        guarded = _bounded_int(
            "LITERATURE_MEMORY_GUARDED_BROWSER_COUNT",
            guarded_default,
            1,
            normal,
        )
        constrained = _bounded_int(
            "LITERATURE_MEMORY_CONSTRAINED_BROWSER_COUNT",
            constrained_default,
            1,
            guarded,
        )
        critical = _bounded_int(
            "LITERATURE_MEMORY_CRITICAL_BROWSER_COUNT",
            critical_default,
            1,
            constrained,
        )
        guarded_available_bytes = _bounded_int(
            "LITERATURE_MEMORY_GUARDED_AVAILABLE_BYTES",
            20 * GIB,
            GIB,
            1024 * GIB,
        )
        constrained_available_bytes = min(
            guarded_available_bytes,
            _bounded_int(
                "LITERATURE_MEMORY_CONSTRAINED_AVAILABLE_BYTES",
                14 * GIB,
                GIB,
                1024 * GIB,
            ),
        )
        critical_available_bytes = min(
            constrained_available_bytes,
            _bounded_int(
                "LITERATURE_MEMORY_CRITICAL_AVAILABLE_BYTES",
                8 * GIB,
                GIB,
                1024 * GIB,
            ),
        )
        guarded_available_ratio = _bounded_float(
            "LITERATURE_MEMORY_GUARDED_AVAILABLE_RATIO", 0.35, 0.01, 0.95
        )
        constrained_available_ratio = min(
            guarded_available_ratio,
            _bounded_float(
                "LITERATURE_MEMORY_CONSTRAINED_AVAILABLE_RATIO",
                0.25,
                0.01,
                0.95,
            ),
        )
        critical_available_ratio = min(
            constrained_available_ratio,
            _bounded_float(
                "LITERATURE_MEMORY_CRITICAL_AVAILABLE_RATIO",
                0.15,
                0.01,
                0.95,
            ),
        )
        return cls(
            maximum=maximum,
            normal_count=normal,
            guarded_count=guarded,
            constrained_count=constrained,
            critical_count=critical,
            guarded_available_bytes=guarded_available_bytes,
            constrained_available_bytes=constrained_available_bytes,
            critical_available_bytes=critical_available_bytes,
            guarded_available_ratio=guarded_available_ratio,
            constrained_available_ratio=constrained_available_ratio,
            critical_available_ratio=critical_available_ratio,
        )

    def evaluate(self, snapshot: MemorySnapshot | None) -> dict[str, Any]:
        if snapshot is None:
            return {
                "pressure": "unknown",
                "target": self.guarded_count,
                "total_bytes": None,
                "available_bytes": None,
                "available_ratio": None,
            }
        available = snapshot.available_bytes
        ratio = snapshot.available_ratio
        if (
            available <= self.critical_available_bytes
            or ratio <= self.critical_available_ratio
        ):
            pressure = "critical"
            target = self.critical_count
        elif (
            available <= self.constrained_available_bytes
            or ratio <= self.constrained_available_ratio
        ):
            pressure = "constrained"
            target = self.constrained_count
        elif (
            available <= self.guarded_available_bytes
            or ratio <= self.guarded_available_ratio
        ):
            pressure = "guarded"
            target = self.guarded_count
        else:
            pressure = "normal"
            target = self.normal_count
        return {
            "pressure": pressure,
            "target": target,
            "total_bytes": snapshot.total_bytes,
            "available_bytes": snapshot.available_bytes,
            "available_ratio": round(snapshot.available_ratio, 6),
        }


class CachedMemoryGuard:
    def __init__(
        self,
        policy: MemoryCapacityPolicy,
        *,
        sample_seconds: float = 5.0,
        reader: Callable[[], MemorySnapshot] = read_memory_snapshot,
    ) -> None:
        self.policy = policy
        self.sample_seconds = max(0.1, float(sample_seconds))
        self.reader = reader
        self._lock = threading.Lock()
        self._sampled_at = 0.0
        self._decision: dict[str, Any] | None = None

    def decision(self, *, force: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            if (
                not force
                and self._decision is not None
                and now - self._sampled_at < self.sample_seconds
            ):
                return dict(self._decision)
            try:
                snapshot: MemorySnapshot | None = self.reader()
            except (OSError, ValueError):
                snapshot = None
            self._decision = self.policy.evaluate(snapshot)
            self._sampled_at = now
            return dict(self._decision)
