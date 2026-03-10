"""Compile tool: build the Go project in ./output/."""

import subprocess
import time
from pathlib import Path

from .base import BaseTool, ToolResult


class CompileTool(BaseTool):
    def __init__(self, output_dir: Path, go_binary: str = "go", timeout: int = 120):
        self._output_dir = output_dir
        self._go_binary = go_binary
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "compile"

    @property
    def description(self) -> str:
        return (
            "Compile the Go project in the output directory. "
            "Builds all packages and reports any compilation errors with file:line:message. "
            "Supports incremental compilation (Go's build cache handles this)."
        )

    @property
    def input_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "packages": {
                    "type": "string",
                    "description": "Go packages to build, e.g. './...' for all. Default: './...'",
                },
                "race": {
                    "type": "boolean",
                    "description": "Enable race detector. Default: false",
                },
                "tags": {
                    "type": "string",
                    "description": "Build tags, e.g. 'instrumentation'",
                },
            },
        }

    async def execute(self, **kwargs) -> ToolResult:
        packages = kwargs.get("packages", "./...")
        race = kwargs.get("race", False)
        tags = kwargs.get("tags", "")

        go_mod = self._output_dir / "go.mod"
        if not go_mod.exists():
            return ToolResult(
                success=False,
                output="",
                error="No go.mod found in output directory. Generate the module first.",
            )

        cmd = [self._go_binary, "build"]
        if race:
            cmd.append("-race")
        if tags:
            cmd.extend(["-tags", tags])
        cmd.append(packages)

        build_dir = (self._output_dir / "build").resolve()
        build_dir.mkdir(parents=True, exist_ok=True)

        import os
        base_path = os.environ.get("PATH", "/usr/bin:/bin")
        import os as _os
        go_bin = _os.path.expanduser("~/go/bin")
        extra = f"{go_bin}:/opt/homebrew/bin:/opt/homebrew/opt/openjdk/bin:/usr/local/go/bin"
        env = {
            "GOPATH": str(build_dir / "gopath"),
            "GOCACHE": str(build_dir / "cache"),
            "HOME": str(Path.home()),
            "PATH": f"{extra}:{base_path}",
        }

        start = time.monotonic()
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self._output_dir.resolve()),
                capture_output=True,
                text=True,
                timeout=self._timeout,
                env=env,
            )
            elapsed = time.monotonic() - start

            if result.returncode == 0:
                return ToolResult(
                    success=True,
                    output=f"Build succeeded in {elapsed:.1f}s\n{result.stdout}".strip(),
                    metrics={"compile_time_s": round(elapsed, 2)},
                )
            else:
                errors = self._parse_errors(result.stderr)
                return ToolResult(
                    success=False,
                    output=result.stdout.strip(),
                    error=errors or result.stderr.strip(),
                    metrics={"compile_time_s": round(elapsed, 2)},
                )
        except subprocess.TimeoutExpired:
            return ToolResult(
                success=False,
                output="",
                error=f"Compilation timed out after {self._timeout}s",
            )
        except Exception as e:
            return ToolResult(success=False, output="", error=str(e))

    def _parse_errors(self, stderr: str) -> str:
        lines = []
        for line in stderr.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                lines.append(stripped)
        return "\n".join(lines[:50])
