"""Test tool: run functional, benchmark, linearizability, and TLC tests."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

from .base import BaseTool, ToolResult


def _find_java() -> str:
    """Find a working java binary, preferring Homebrew installations."""
    # Check Homebrew locations first (macOS stub at /usr/bin/java often fails)
    for candidate in [
        "/opt/homebrew/opt/openjdk/bin/java",
        "/opt/homebrew/bin/java",
        "/usr/local/opt/openjdk/bin/java",
    ]:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    java = shutil.which("java")
    if java:
        return java
    return "java"


class TestTool(BaseTool):
    def __init__(self, output_dir: Path, config=None):
        self._output_dir = output_dir
        self._config = config

    @property
    def name(self) -> str:
        return "test"

    @property
    def description(self) -> str:
        return (
            "Run tests on the KV store. Four modes:\n"
            "  --functional: Single-node CRUD correctness (2-3s)\n"
            "  --bench: YCSB benchmark with workload (30-60s). Returns throughput, p50/p95/p99.\n"
            "  --linearizability: Full cluster + fault injection + Porcupine (3-5min). Returns PASS/FAIL.\n"
            "  --tlc: TLC model checker on TLA+ spec. Returns states explored, violations."
        )

    @property
    def input_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": ["functional", "bench", "linearizability", "tlc"],
                    "description": "Test mode to run.",
                },
                "nodes": {
                    "type": "integer",
                    "description": "Number of cluster nodes (for bench/linearizability). Default: 3",
                },
                "shards": {
                    "type": "integer",
                    "description": "Number of shards. Default: 3",
                },
                "duration": {
                    "type": "integer",
                    "description": "Benchmark duration in seconds. Default: 30",
                },
                "workers": {
                    "type": "integer",
                    "description": "Concurrent client workers. Default: 8",
                },
                "crash_node": {
                    "type": "boolean",
                    "description": "Crash a node during linearizability test. Default: true",
                },
                "tlc_config": {
                    "type": "string",
                    "description": "TLC config file path (relative to output). Default: tlaplus/KVStore.cfg",
                },
            },
            "required": ["mode"],
        }

    async def execute(self, **kwargs) -> ToolResult:
        mode = kwargs.get("mode", "functional")
        dispatch = {
            "functional": self._run_functional,
            "bench": self._run_bench,
            "linearizability": self._run_linearizability,
            "tlc": self._run_tlc,
        }
        handler = dispatch.get(mode)
        if not handler:
            return ToolResult(success=False, output="", error=f"Unknown test mode: {mode}")
        return handler(kwargs)

    def _run_functional(self, opts: dict) -> ToolResult:
        timeout = 30
        if self._config:
            timeout = self._config.test_functional_timeout

        test_script = self._output_dir / "test_functional.sh"
        if test_script.exists():
            cmd = ["bash", str(test_script)]
        else:
            cmd = ["go", "test", "-v", "-count=1", "-timeout", f"{timeout}s", "./..."]

        start = time.monotonic()
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self._output_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            elapsed = time.monotonic() - start
            output = (result.stdout + "\n" + result.stderr).strip()

            return ToolResult(
                success=result.returncode == 0,
                output=output[:50_000],
                error="" if result.returncode == 0 else f"Tests failed (exit {result.returncode})",
                metrics={"test_time_s": round(elapsed, 2), "mode": "functional"},
            )
        except subprocess.TimeoutExpired:
            return ToolResult(success=False, output="", error=f"Functional tests timed out after {timeout}s")

    def _run_bench(self, opts: dict) -> ToolResult:
        nodes = opts.get("nodes", 3)
        shards = opts.get("shards", 3)
        duration = opts.get("duration", 30)
        workers = opts.get("workers", 8)
        timeout = duration + 60
        if self._config:
            timeout = self._config.test_bench_timeout

        bench_script = self._output_dir / "run_bench.sh"
        if bench_script.exists():
            cmd = [
                "bash", str(bench_script),
                str(nodes), str(shards), str(duration), str(workers),
            ]
        else:
            cmd = [
                "go", "run", "./ycsb/cmd/ycsbrun/main.go",
                "-nodes", str(nodes),
                "-shards", str(shards),
                "-duration", str(duration),
                "-workers", str(workers),
            ]

        start = time.monotonic()
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self._output_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            elapsed = time.monotonic() - start
            output = (result.stdout + "\n" + result.stderr).strip()
            metrics = self._parse_bench_output(output)
            metrics["bench_time_s"] = round(elapsed, 2)
            metrics["mode"] = "bench"

            return ToolResult(
                success=result.returncode == 0,
                output=output[:50_000],
                error="" if result.returncode == 0 else f"Benchmark failed (exit {result.returncode})",
                metrics=metrics,
            )
        except subprocess.TimeoutExpired:
            return ToolResult(success=False, output="", error=f"Benchmark timed out after {timeout}s")

    def _run_linearizability(self, opts: dict) -> ToolResult:
        nodes = opts.get("nodes", 3)
        shards = opts.get("shards", 3)
        workers = opts.get("workers", 8)
        crash = opts.get("crash_node", True)
        timeout = 600
        if self._config:
            timeout = self._config.test_linearizability_timeout

        harness_script = self._output_dir / "run_linearizability.sh"
        if harness_script.exists():
            cmd = [
                "bash", str(harness_script),
                str(nodes), str(shards), str(workers),
                "1" if crash else "0",
            ]
        else:
            cmd_parts = [
                "go", "run", "./correctness/cmd/harness/main.go",
                "-nodes", str(nodes),
                "-shards", str(shards),
                "-workers", str(workers),
            ]
            if crash:
                cmd_parts.extend(["-crash", "1"])
            cmd = cmd_parts

        start = time.monotonic()
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self._output_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            elapsed = time.monotonic() - start
            output = (result.stdout + "\n" + result.stderr).strip()

            passed = result.returncode == 0 and "PASS" in output.upper()

            return ToolResult(
                success=passed,
                output=output[:50_000],
                error="" if passed else "Linearizability check FAILED",
                metrics={
                    "linearizability_time_s": round(elapsed, 2),
                    "mode": "linearizability",
                    "passed": passed,
                },
            )
        except subprocess.TimeoutExpired:
            return ToolResult(
                success=False, output="", error=f"Linearizability test timed out after {timeout}s"
            )

    def _run_tlc(self, opts: dict) -> ToolResult:
        tlc_config = opts.get("tlc_config", "tlaplus/KVStore.cfg")
        timeout = 300
        if self._config:
            timeout = self._config.test_tlc_timeout

        tlc_jar = (self._output_dir / "tlaplus" / "tla2tools.jar").resolve()
        if self._config and self._config.tlc_jar:
            candidate = Path(self._config.tlc_jar).resolve()
            if candidate.exists():
                tlc_jar = candidate
        if not tlc_jar.exists():
            return ToolResult(success=False, output="", error=f"TLC jar not found at {tlc_jar}")
        tlc_jar = str(tlc_jar)

        cfg_path = (self._output_dir / tlc_config).resolve()
        if not cfg_path.exists():
            return ToolResult(success=False, output="", error=f"TLC config not found: {tlc_config}")

        tla_dir = cfg_path.parent
        cfg_name = cfg_path.name
        spec_name = cfg_name.replace(".cfg", ".tla")

        java_bin = _find_java()

        cmd = [
            java_bin, "-XX:+UseParallelGC",
            "-jar", tlc_jar,
            "-config", cfg_name,
            "-workers", "auto",
            "-nowarning",
            str(tla_dir / spec_name),
        ]

        start = time.monotonic()
        try:
            result = subprocess.run(
                cmd,
                cwd=str(tla_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            elapsed = time.monotonic() - start
            output = (result.stdout + "\n" + result.stderr).strip()

            has_violation = (
                "is violated" in output.lower()
                or "invariant" in output.lower() and "violated" in output.lower()
                or result.returncode == 12  # TLC exit code for property violation
            )
            has_error = (
                "semantic error" in output.lower()
                or "parsing" in output.lower() and "failed" in output.lower()
                or "unable to" in output.lower()
            )
            model_check_ok = "model checking completed. no error" in output.lower()
            is_bug_mode = "bug" in tlc_config.lower()

            if has_error:
                success = False
                msg = "TLC encountered a spec error (not a property violation)"
            elif is_bug_mode:
                success = has_violation
                msg = "Bug mode: counterexample found (expected)" if success else "Bug mode: no violation found (unexpected)"
            else:
                success = model_check_ok and not has_violation
                msg = "Model check passed" if success else "Model check found violations"

            return ToolResult(
                success=success,
                output=f"{msg}\n\n{output[:50_000]}",
                error="" if success else msg,
                metrics={
                    "tlc_time_s": round(elapsed, 2),
                    "mode": "tlc",
                    "bug_mode": is_bug_mode,
                    "violation_found": has_violation,
                },
            )
        except subprocess.TimeoutExpired:
            return ToolResult(success=False, output="", error=f"TLC timed out after {timeout}s")
        except FileNotFoundError:
            return ToolResult(success=False, output="", error="Java not found. TLC requires Java.")

    def _parse_bench_output(self, output: str) -> dict:
        metrics: dict = {}
        for line in output.splitlines():
            lower = line.lower()
            if "throughput" in lower or "ops/sec" in lower:
                parts = line.split()
                for i, p in enumerate(parts):
                    try:
                        val = float(p.replace(",", ""))
                        if val > 100:
                            metrics["throughput_ops"] = val
                            break
                    except ValueError:
                        continue
            for tag in ["p50", "p95", "p99"]:
                if tag in lower:
                    parts = line.split()
                    for p in parts:
                        try:
                            val = float(p.replace("ms", "").replace("us", "").replace(",", ""))
                            if val > 0:
                                unit = "ms"
                                if "us" in line.lower() or "µs" in line.lower():
                                    unit = "us"
                                metrics[f"{tag}_latency_{unit}"] = val
                                break
                        except ValueError:
                            continue
        return metrics
