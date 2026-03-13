import argparse
import itertools
import re
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DESIGN_DIR = Path(__file__).resolve().parent
SYSTEM_PROMPT_PATH = DESIGN_DIR / "architecture_design.md"
JUDGE_PROMPT_PATH = DESIGN_DIR / "architecture_design_judge.md"
STORAGE_PLAN_PATH = DESIGN_DIR.parent / "1_workload_contract" / "storage_plan.txt"
OUTPUT_PATH = DESIGN_DIR / "design_spec.txt"
LOGS_PATH = DESIGN_DIR / "log.txt"

_models_dir = REPO_ROOT / "tools" / "models"
if str(_models_dir) not in sys.path:
    sys.path.insert(0, str(_models_dir))


def _load_file(path: Path, label: str) -> str:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path.read_text().strip()


class _Spinner:
    def __init__(self, message: str = "Waiting for model to respond"):
        self._message = message
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> "_Spinner":
        self._stop.clear()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()
        return self

    def _spin(self) -> None:
        chars = itertools.cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
        start = time.time()
        while not self._stop.is_set():
            elapsed = int(time.time() - start)
            print(f"\r{next(chars)} {self._message}... ({elapsed}s)", end="", file=sys.stderr, flush=True)
            self._stop.wait(0.1)
        print("\r" + " " * 60 + "\r", end="", file=sys.stderr, flush=True)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join()


def _call_claude(prompt: str, system: str, max_tokens: int = 8192) -> str:
    from claude import run
    return run(prompt, system=system, max_tokens=max_tokens)


def _call_openai(prompt: str, system: str) -> str:
    from openai import run
    return run(prompt, instructions=system)


def _get_call_fn(provider: str):
    if provider == "claude":
        return _call_claude
    elif provider == "openai":
        return _call_openai
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _build_design_prompt(storage_plan: str, user_prompt: str | None) -> str:
    parts = [f"<SYSTEM_INPUT>\n{storage_plan}\n</SYSTEM_INPUT>"]
    if user_prompt:
        parts.append(f"\n<ADDITIONAL_CONTEXT>\n{user_prompt}\n</ADDITIONAL_CONTEXT>")
    return "\n".join(parts)


def _build_judge_prompt(storage_plan: str, candidates: list[str]) -> str:
    parts = ["<INPUT>"]
    parts.append(f"<SYSTEM_INPUT>\n{storage_plan}\n</SYSTEM_INPUT>")
    parts.append("<CANDIDATES>")
    for i, candidate in enumerate(candidates, 1):
        parts.append(f"<CANDIDATE_{i}>\n{candidate}\n</CANDIDATE_{i}>")
    parts.append("</CANDIDATES>")
    parts.append("</INPUT>")
    return "\n".join(parts)


def _extract_winner_number(judge_response: str) -> int | None:
    match = re.search(r"<WINNER>\s*Candidate\s+(\d+)\s*</WINNER>", judge_response, re.DOTALL)
    if match:
        return int(match.group(1))
    return None


def _log(logs: list[str], entry: str) -> None:
    logs.append(entry)


def _clean_previous_run() -> None:
    for path in [OUTPUT_PATH, LOGS_PATH]:
        if path.exists():
            path.unlink()


def run_pipeline(
    num_specs: int = 3,
    design_provider: str = "claude",
    judge_provider: str = "claude",
    user_prompt: str | None = None,
) -> str:
    _clean_previous_run()
    system_prompt = _load_file(SYSTEM_PROMPT_PATH, "Architecture design system prompt")
    judge_prompt = _load_file(JUDGE_PROMPT_PATH, "Judge system prompt")
    storage_plan = _load_file(STORAGE_PLAN_PATH, "Storage plan")

    design_call = _get_call_fn(design_provider)
    judge_call = _get_call_fn(judge_provider)

    prompt = _build_design_prompt(storage_plan, user_prompt)
    logs: list[str] = []

    _log(logs, f"Pipeline started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    _log(logs, f"Design provider: {design_provider}")
    _log(logs, f"Judge provider:  {judge_provider}")
    _log(logs, f"Candidates:      {num_specs}")
    _log(logs, "=" * 60)

    candidates: list[str] = []
    for i in range(1, num_specs + 1):
        print(f"\n{'='*50}", file=sys.stderr)
        print(f"  Generating candidate {i} of {num_specs}  ({design_provider})", file=sys.stderr)
        print(f"{'='*50}\n", file=sys.stderr)

        spinner = _Spinner(f"Candidate {i} — thinking").start()
        try:
            response = design_call(prompt, system_prompt)
        finally:
            spinner.stop()

        candidates.append(response)
        _log(logs, f"\n{'='*60}")
        _log(logs, f"CANDIDATE {i} ({design_provider})")
        _log(logs, f"{'='*60}")
        _log(logs, response)

        print(f"  Candidate {i} received ({len(response)} chars).\n", file=sys.stderr)

    if num_specs == 1:
        winner_idx = 1
        winner_response = candidates[0]
        _log(logs, f"\n{'='*60}")
        _log(logs, "JUDGE PHASE SKIPPED (only 1 candidate)")
        _log(logs, f"{'='*60}")
        print("\n  Only 1 candidate — skipping judge phase.\n", file=sys.stderr)
    else:
        print(f"\n{'='*50}", file=sys.stderr)
        print(f"  Judging {num_specs} candidates  ({judge_provider})", file=sys.stderr)
        print(f"{'='*50}\n", file=sys.stderr)

        judge_user_prompt = _build_judge_prompt(storage_plan, candidates)

        spinner = _Spinner("Judge — evaluating candidates").start()
        try:
            judge_response = judge_call(judge_user_prompt, judge_prompt)
        finally:
            spinner.stop()

        _log(logs, f"\n{'='*60}")
        _log(logs, f"JUDGE RESPONSE ({judge_provider})")
        _log(logs, f"{'='*60}")
        _log(logs, judge_response)

        winner_idx = _extract_winner_number(judge_response)
        if winner_idx is None or winner_idx < 1 or winner_idx > num_specs:
            print(f"  Warning: could not parse winner from judge. Defaulting to candidate 1.", file=sys.stderr)
            _log(logs, "WARNING: Could not parse winner. Defaulting to candidate 1.")
            winner_idx = 1

        winner_response = candidates[winner_idx - 1]
        print(f"  Winner: Candidate {winner_idx}\n", file=sys.stderr)

    OUTPUT_PATH.write_text(winner_response)
    _log(logs, f"\n{'='*60}")
    _log(logs, f"WINNER: Candidate {winner_idx}")
    _log(logs, f"Output saved to: {OUTPUT_PATH}")
    _log(logs, f"Pipeline finished: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    LOGS_PATH.write_text("\n".join(logs) + "\n")

    print(f"  Design spec saved to: {OUTPUT_PATH}", file=sys.stderr)
    print(f"  Full log saved to:    {LOGS_PATH}", file=sys.stderr)
    print("\nDone! Your architecture design spec is ready.\n", file=sys.stderr)
    return winner_response


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate architecture design specs and judge the best one."
    )
    parser.add_argument(
        "-n", "--num-specs",
        type=int,
        default=3,
        help="Number of candidate specs to generate (default: 3)",
    )
    parser.add_argument(
        "--design-provider",
        choices=["claude", "openai"],
        default="claude",
        help="LLM provider for generating design specs (default: claude)",
    )
    parser.add_argument(
        "--judge-provider",
        choices=["claude", "openai"],
        default="claude",
        help="LLM provider for the judge (default: claude)",
    )
    parser.add_argument(
        "-p", "--prompt",
        type=str,
        default=None,
        help="Additional context or instructions for the design LLM",
    )
    parser.add_argument(
        "-f", "--prompt-file",
        type=Path,
        default=None,
        help="Read additional context from a file",
    )
    args = parser.parse_args()

    if args.num_specs < 1:
        print("Error: --num-specs must be at least 1.", file=sys.stderr)
        return 1

    user_prompt = None
    if args.prompt_file is not None:
        user_prompt = args.prompt_file.read_text().strip()
    elif args.prompt:
        user_prompt = args.prompt

    try:
        result = run_pipeline(
            num_specs=args.num_specs,
            design_provider=args.design_provider,
            judge_provider=args.judge_provider,
            user_prompt=user_prompt,
        )
        print(result)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
