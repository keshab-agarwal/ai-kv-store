"""Shell tool: run arbitrary commands in ./output/."""

import os
import subprocess
from pathlib import Path

from .base import BaseTool, ToolResult


class ShellTool(BaseTool):
    def __init__(self, output_dir: Path, timeout: int = 60):
        self._output_dir = output_dir
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "shell"

    @property
    def description(self) -> str:
        return (
            "Run a shell command in the output directory. "
            "Use for: inspecting code (grep, cat, find), examining data, "
            "running custom scripts, analyzing profiling output, running TLC. "
            "Commands run with bash. Working directory is ./output/."
        )

    @property
    def input_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to execute.",
                },
                "timeout": {
                    "type": "integer",
                    "description": f"Timeout in seconds. Default: {self._timeout}",
                },
                "working_dir": {
                    "type": "string",
                    "description": "Subdirectory within output to run from. Default: root of output.",
                },
            },
            "required": ["command"],
        }

    async def execute(self, **kwargs) -> ToolResult:
        command = kwargs.get("command", "")
        timeout = kwargs.get("timeout", self._timeout)
        working_dir = kwargs.get("working_dir", "")

        if not command:
            return ToolResult(success=False, output="", error="No command provided")

        blocked = ["rm -rf /", "rm -rf ~", ":(){ :|:& };:"]
        for pattern in blocked:
            if pattern in command:
                return ToolResult(success=False, output="", error=f"Blocked dangerous command: {pattern}")

        cwd = self._output_dir
        if working_dir:
            cwd = cwd / working_dir

        cwd.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        go_bin = os.path.expanduser("~/go/bin")
        extra_paths = [go_bin, "/opt/homebrew/opt/openjdk/bin", "/opt/homebrew/bin", "/usr/local/go/bin"]
        env["PATH"] = ":".join(extra_paths) + ":" + env.get("PATH", "")

        try:
            result = subprocess.run(
                ["bash", "-c", command],
                cwd=str(cwd),
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )

            combined = ""
            if result.stdout:
                combined += result.stdout
            if result.stderr:
                if combined:
                    combined += "\n--- stderr ---\n"
                combined += result.stderr

            truncated = combined[:100_000]
            if len(combined) > 100_000:
                truncated += f"\n... (truncated, {len(combined)} total chars)"

            return ToolResult(
                success=result.returncode == 0,
                output=truncated,
                error="" if result.returncode == 0 else f"Exit code: {result.returncode}",
            )
        except subprocess.TimeoutExpired:
            return ToolResult(success=False, output="", error=f"Command timed out after {timeout}s")
        except Exception as e:
            return ToolResult(success=False, output="", error=str(e))
