"""Git-based snapshotting for the output directory."""

import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


class GitSnapshotter:
    def __init__(self, repo_dir: Path):
        self._repo_dir = repo_dir
        self._initialized = False

    def init(self):
        self._repo_dir.mkdir(parents=True, exist_ok=True)
        git_dir = self._repo_dir / ".git"
        if not git_dir.exists():
            self._run("git", "init")
            self._run("git", "config", "user.email", "pipeline@bespoke-kv.local")
            self._run("git", "config", "user.name", "Bespoke KV Pipeline")
            gitignore = self._repo_dir / ".gitignore"
            gitignore.write_text("build/\n*.log\n__pycache__/\n")
            self._run("git", "add", "-A")
            self._run("git", "commit", "-m", "Initial snapshot", "--allow-empty")
        self._initialized = True

    def snapshot(self, message: str, tag: str | None = None) -> str:
        if not self._initialized:
            self.init()

        self._run("git", "add", "-A")

        status = self._run("git", "status", "--porcelain")
        if not status.strip():
            log.info("No changes to snapshot")
            return self._current_hash()

        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        full_msg = f"[{ts}] {message}"
        self._run("git", "commit", "-m", full_msg)
        commit_hash = self._current_hash()

        if tag:
            safe_tag = tag.replace(" ", "-").replace("/", "-")
            self._run("git", "tag", "-f", safe_tag)
            log.info("Snapshot %s tagged as %s", commit_hash[:8], safe_tag)

        return commit_hash

    def rollback(self, ref: str) -> bool:
        try:
            self._run("git", "checkout", ref, "--", ".")
            self._run("git", "add", "-A")
            self._run("git", "commit", "-m", f"Rollback to {ref}", "--allow-empty")
            log.info("Rolled back to %s", ref)
            return True
        except subprocess.CalledProcessError as e:
            log.error("Rollback to %s failed: %s", ref, e)
            return False

    def rollback_last(self) -> bool:
        try:
            self._run("git", "reset", "--hard", "HEAD~1")
            log.info("Rolled back to previous snapshot")
            return True
        except subprocess.CalledProcessError:
            log.error("Failed to rollback to previous snapshot")
            return False

    def list_snapshots(self) -> list[dict]:
        try:
            out = self._run(
                "git", "log", "--oneline", "--decorate", "--all", "-50"
            )
            snapshots = []
            for line in out.strip().splitlines():
                parts = line.split(" ", 1)
                if len(parts) == 2:
                    snapshots.append({"hash": parts[0], "message": parts[1]})
            return snapshots
        except subprocess.CalledProcessError:
            return []

    def list_tags(self) -> list[str]:
        try:
            out = self._run("git", "tag", "-l")
            return [t.strip() for t in out.strip().splitlines() if t.strip()]
        except subprocess.CalledProcessError:
            return []

    def diff_from(self, ref: str) -> str:
        try:
            return self._run("git", "diff", ref, "HEAD")
        except subprocess.CalledProcessError:
            return ""

    def _current_hash(self) -> str:
        return self._run("git", "rev-parse", "HEAD").strip()

    def _run(self, *args: str) -> str:
        result = subprocess.run(
            list(args),
            cwd=str(self._repo_dir),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode, args, result.stdout, result.stderr
            )
        return result.stdout
