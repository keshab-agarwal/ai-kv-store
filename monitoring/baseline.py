"""Baseline tracking: records and persists performance/correctness baselines."""

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    timestamp: float = 0.0
    throughput_ops: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    correctness_pass: bool = False
    snapshot_hash: str = ""
    stage: str = ""
    notes: str = ""
    raw_metrics: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


class BaselineTracker:
    def __init__(self, storage_path: Path):
        self._path = storage_path
        self._history: list[BenchmarkResult] = []
        self._baseline: BenchmarkResult | None = None
        self._load()

    def record(self, result: BenchmarkResult):
        self._history.append(result)
        self._save()
        log.info(
            "Recorded result: throughput=%.0f ops/s, p99=%.2fms, correct=%s",
            result.throughput_ops,
            result.p99_latency_ms,
            result.correctness_pass,
        )

    def set_baseline(self, result: BenchmarkResult):
        self._baseline = result
        self._save()
        log.info(
            "Baseline set: throughput=%.0f ops/s, p99=%.2fms",
            result.throughput_ops,
            result.p99_latency_ms,
        )

    @property
    def baseline(self) -> BenchmarkResult | None:
        return self._baseline

    @property
    def history(self) -> list[BenchmarkResult]:
        return list(self._history)

    def last_n(self, n: int = 5) -> list[BenchmarkResult]:
        return self._history[-n:]

    def _save(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "baseline": asdict(self._baseline) if self._baseline else None,
            "history": [asdict(r) for r in self._history],
        }
        self._path.write_text(json.dumps(data, indent=2))

    def _load(self):
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text())
            if data.get("baseline"):
                self._baseline = BenchmarkResult(**data["baseline"])
            self._history = [BenchmarkResult(**r) for r in data.get("history", [])]
        except (json.JSONDecodeError, TypeError) as e:
            log.warning("Failed to load baseline data: %s", e)
