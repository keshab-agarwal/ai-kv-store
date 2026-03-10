"""Porcupine linearizability checker integration."""

import json
import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)


class PorcupineValidator:
    def __init__(self, output_dir: Path, go_binary: str = "go", timeout: int = 300):
        self._output_dir = output_dir
        self._go_binary = go_binary
        self._timeout = timeout

    def run_checker(self, history_file: Path | None = None) -> tuple[bool, str]:
        checker_dir = self._output_dir / "correctness" / "cmd" / "checker"
        if not checker_dir.exists():
            return False, "Checker directory not found. Generate it first."

        cmd = [self._go_binary, "run", "."]
        if history_file:
            cmd.extend(["-history", str(history_file)])

        try:
            result = subprocess.run(
                cmd,
                cwd=str(checker_dir),
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
            output = (result.stdout + "\n" + result.stderr).strip()
            passed = result.returncode == 0 and "PASS" in output.upper()
            return passed, output
        except subprocess.TimeoutExpired:
            return False, f"Porcupine checker timed out after {self._timeout}s"

    def run_self_test(self) -> tuple[bool, str]:
        checker_dir = self._output_dir / "correctness" / "cmd" / "checker"
        if not checker_dir.exists():
            return False, "Checker directory not found"

        cmd = [self._go_binary, "run", ".", "-selftest"]
        try:
            result = subprocess.run(
                cmd,
                cwd=str(checker_dir),
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
            output = (result.stdout + "\n" + result.stderr).strip()
            passed = result.returncode == 0
            return passed, output
        except subprocess.TimeoutExpired:
            return False, "Self-test timed out"

    def run_full_harness(
        self,
        nodes: int = 3,
        shards: int = 3,
        workers: int = 8,
        crash_node: bool = True,
        seed: int | None = None,
    ) -> tuple[bool, str, dict]:
        harness_dir = self._output_dir / "correctness" / "cmd" / "harness"
        if not harness_dir.exists():
            return False, "Harness directory not found", {}

        cmd = [
            self._go_binary, "run", ".",
            "-nodes", str(nodes),
            "-shards", str(shards),
            "-workers", str(workers),
        ]
        if crash_node:
            cmd.extend(["-crash", "1"])
        if seed is not None:
            cmd.extend(["-seed", str(seed)])

        try:
            result = subprocess.run(
                cmd,
                cwd=str(harness_dir),
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
            output = (result.stdout + "\n" + result.stderr).strip()
            passed = result.returncode == 0 and "PASS" in output.upper()

            metrics = self._parse_harness_output(output)
            return passed, output, metrics
        except subprocess.TimeoutExpired:
            return False, f"Harness timed out after {self._timeout}s", {}

    def _parse_harness_output(self, output: str) -> dict:
        metrics = {}
        for line in output.splitlines():
            if ":" in line:
                key, _, val = line.partition(":")
                key = key.strip().lower().replace(" ", "_")
                val = val.strip()
                try:
                    metrics[key] = int(val)
                except ValueError:
                    try:
                        metrics[key] = float(val)
                    except ValueError:
                        if val.upper() in ("PASS", "FAIL", "TRUE", "FALSE"):
                            metrics[key] = val.upper()
        return metrics
