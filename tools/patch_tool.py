"""Patch tool: apply unified diffs to files in ./output/."""

import subprocess
import tempfile
from pathlib import Path

from .base import BaseTool, ToolResult


class PatchTool(BaseTool):
    def __init__(self, output_dir: Path):
        self._output_dir = output_dir

    @property
    def name(self) -> str:
        return "patch"

    @property
    def description(self) -> str:
        return (
            "Apply a code change to files in the output directory. "
            "Accepts either a unified diff (output of `diff -u`) or full file content. "
            "When using mode='full', the entire file_path is overwritten with the content. "
            "When using mode='diff' (default), a unified diff is applied with `patch`."
        )

    @property
    def input_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": (
                        "Relative path within ./output/ for the file to create or modify. "
                        "Required for mode='full'. Optional for mode='diff' if the diff "
                        "contains file paths."
                    ),
                },
                "content": {
                    "type": "string",
                    "description": "The unified diff (mode='diff') or full file content (mode='full').",
                },
                "mode": {
                    "type": "string",
                    "enum": ["diff", "full"],
                    "description": "Whether to apply a unified diff or write full file content. Default: 'full'.",
                },
            },
            "required": ["content"],
        }

    async def execute(self, **kwargs) -> ToolResult:
        content = kwargs.get("content", "")
        mode = kwargs.get("mode", "full")
        file_path = kwargs.get("file_path", "")

        if mode == "full":
            return self._write_full(file_path, content)
        else:
            return self._apply_diff(content, file_path)

    def _write_full(self, rel_path: str, content: str) -> ToolResult:
        if not rel_path:
            return ToolResult(success=False, output="", error="file_path is required for mode='full'")
        target = self._output_dir / rel_path
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
            return ToolResult(success=True, output=f"Wrote {len(content)} bytes to {rel_path}")
        except Exception as e:
            return ToolResult(success=False, output="", error=str(e))

    def _apply_diff(self, diff_text: str, file_path: str) -> ToolResult:
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".patch", delete=False) as f:
                f.write(diff_text)
                patch_file = f.name

            cmd = ["patch", "-p1", "--forward", "--no-backup-if-mismatch", "-i", patch_file]
            if file_path:
                target = self._output_dir / file_path
                if not target.exists():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.touch()
                cmd = ["patch", "--forward", "--no-backup-if-mismatch", "-i", patch_file, str(target)]

            result = subprocess.run(
                cmd,
                cwd=str(self._output_dir),
                capture_output=True,
                text=True,
                timeout=30,
            )

            Path(patch_file).unlink(missing_ok=True)

            if result.returncode == 0:
                return ToolResult(success=True, output=result.stdout.strip())
            else:
                return ToolResult(
                    success=False,
                    output=result.stdout.strip(),
                    error=result.stderr.strip() or f"patch exited with code {result.returncode}",
                )
        except subprocess.TimeoutExpired:
            return ToolResult(success=False, output="", error="Patch timed out after 30s")
        except Exception as e:
            return ToolResult(success=False, output="", error=str(e))
