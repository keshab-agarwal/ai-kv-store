"""Functional validation: verify CRUD correctness on a single node."""

import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)


class FunctionalValidator:
    def __init__(self, output_dir: Path, go_binary: str = "go", timeout: int = 30):
        self._output_dir = output_dir
        self._go_binary = go_binary
        self._timeout = timeout

    def run(self, verbose: bool = True) -> tuple[bool, str]:
        go_mod = self._output_dir / "go.mod"
        if not go_mod.exists():
            return False, "No go.mod in output directory"

        cmd = [self._go_binary, "test", "-v" if verbose else "", "-count=1", "-timeout", f"{self._timeout}s"]
        cmd = [c for c in cmd if c]

        test_dirs = self._find_test_dirs()
        if not test_dirs:
            cmd.append("./...")
        else:
            cmd.extend(test_dirs)

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self._output_dir),
                capture_output=True,
                text=True,
                timeout=self._timeout + 10,
            )
            output = (result.stdout + "\n" + result.stderr).strip()
            passed = result.returncode == 0
            if passed:
                log.info("Functional tests PASSED")
            else:
                log.error("Functional tests FAILED")
            return passed, output
        except subprocess.TimeoutExpired:
            return False, f"Tests timed out after {self._timeout}s"
        except Exception as e:
            return False, f"Error running tests: {e}"

    def _find_test_dirs(self) -> list[str]:
        dirs = []
        for f in self._output_dir.rglob("*_test.go"):
            rel = f.parent.relative_to(self._output_dir)
            pkg = "./" + str(rel)
            if pkg not in dirs:
                dirs.append(pkg)
        return dirs
