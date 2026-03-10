"""Regression monitor: detects performance regressions and triggers auto-rollback."""

import logging
from dataclasses import dataclass

from snapshots.git_snapshotter import GitSnapshotter

from .baseline import BaselineTracker, BenchmarkResult

log = logging.getLogger(__name__)


@dataclass
class RegressionVerdict:
    passed: bool
    reason: str
    regressed_metrics: list[str]
    should_rollback: bool


class RegressionMonitor:
    def __init__(
        self,
        tracker: BaselineTracker,
        snapshotter: GitSnapshotter,
        threshold_pct: float = 5.0,
    ):
        self._tracker = tracker
        self._snapshotter = snapshotter
        self._threshold_pct = threshold_pct

    def check(self, result: BenchmarkResult) -> RegressionVerdict:
        self._tracker.record(result)
        baseline = self._tracker.baseline

        if baseline is None:
            log.info("No baseline set; accepting result as first baseline")
            self._tracker.set_baseline(result)
            return RegressionVerdict(
                passed=True,
                reason="First result accepted as baseline",
                regressed_metrics=[],
                should_rollback=False,
            )

        if not result.correctness_pass:
            log.error("Correctness FAILED — triggering rollback")
            return RegressionVerdict(
                passed=False,
                reason="Correctness check failed (linearizability violated)",
                regressed_metrics=["correctness"],
                should_rollback=True,
            )

        regressed = []
        checks = [
            ("p50_latency_ms", baseline.p50_latency_ms, result.p50_latency_ms),
            ("p95_latency_ms", baseline.p95_latency_ms, result.p95_latency_ms),
            ("p99_latency_ms", baseline.p99_latency_ms, result.p99_latency_ms),
        ]
        for name, base_val, new_val in checks:
            if base_val > 0:
                pct_change = ((new_val - base_val) / base_val) * 100
                if pct_change > self._threshold_pct:
                    regressed.append(f"{name}: {base_val:.2f} -> {new_val:.2f} (+{pct_change:.1f}%)")

        if base_val := baseline.throughput_ops:
            pct_change = ((base_val - result.throughput_ops) / base_val) * 100
            if pct_change > self._threshold_pct:
                regressed.append(
                    f"throughput: {base_val:.0f} -> {result.throughput_ops:.0f} (-{pct_change:.1f}%)"
                )

        if regressed:
            log.warning("Performance regression detected:\n  %s", "\n  ".join(regressed))
            return RegressionVerdict(
                passed=False,
                reason="Performance regression detected",
                regressed_metrics=regressed,
                should_rollback=True,
            )

        improved = result.p99_latency_ms < baseline.p99_latency_ms or (
            result.throughput_ops > baseline.throughput_ops
        )
        if improved:
            log.info("Performance improved — updating baseline")
            self._tracker.set_baseline(result)

        return RegressionVerdict(
            passed=True,
            reason="No regression detected" + (" (baseline updated)" if improved else ""),
            regressed_metrics=[],
            should_rollback=False,
        )

    def auto_rollback(self, ref: str | None = None) -> bool:
        if ref:
            return self._snapshotter.rollback(ref)
        return self._snapshotter.rollback_last()

    def get_rollback_message(self, verdict: RegressionVerdict) -> str:
        lines = [
            "REGRESSION DETECTED — Auto-rollback triggered.",
            f"Reason: {verdict.reason}",
            "Regressed metrics:",
        ]
        for m in verdict.regressed_metrics:
            lines.append(f"  - {m}")
        lines.append(
            "The previous optimization was rolled back. "
            "Please try a different optimization approach."
        )
        return "\n".join(lines)
