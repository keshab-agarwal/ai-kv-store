import argparse
import itertools
import re
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EVAL_DIR = Path(__file__).resolve().parent
EVAL_AGENT_PROMPT_PATH = EVAL_DIR / "evalutation.md"
DESIGN_SPEC_PATH = REPO_ROOT / "2_architecture_design" / "design_spec.txt"
OUTPUT_DIR = REPO_ROOT / "output"
BASELINE_DIR = REPO_ROOT / "baseline"
TOOLS_DIR = REPO_ROOT / "tools"
TOOLS_GENERATED_DIR = TOOLS_DIR / "generated"
EVAL_SH_PATH = TOOLS_GENERATED_DIR / "eval.sh"
COMPARISON_PATH = EVAL_DIR / "comparison.txt"
LOGS_PATH = EVAL_DIR / "log.txt"

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
            print(
                f"\r{next(chars)} {self._message}... ({elapsed}s)",
                end="",
                file=sys.stderr,
                flush=True,
            )
            self._stop.wait(0.1)
        print("\r" + " " * 80 + "\r", end="", file=sys.stderr, flush=True)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join()


def _log(logs: list[str], entry: str) -> None:
    logs.append(entry)


def _list_files(directory: Path) -> str:
    if not directory.exists():
        return "(directory does not exist yet)"
    files = sorted(p.relative_to(directory) for p in directory.rglob("*") if p.is_file())
    if not files:
        return "(empty)"
    return "\n".join(str(f) for f in files)


def _call_eval_agent(prompt: str, system: str, provider: str) -> str:
    if provider == "claude":
        from claude import run_agent
        return run_agent(prompt, system_prompt=system, cwd=REPO_ROOT)
    elif provider == "openai":
        from openai_provider import run, get_shell_tool
        return run(prompt, instructions=system, tools=[get_shell_tool()])
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _build_initial_prompt(
    design_spec: str,
    output_state: str,
    baseline_state: str,
    tools_generated_state: str,
    user_prompt: str | None,
) -> str:
    parts = [
        "<DESIGN_SPEC>",
        design_spec,
        "</DESIGN_SPEC>",
        "",
        "<OUTPUT_DIR_STATE>",
        output_state,
        "</OUTPUT_DIR_STATE>",
        "",
        "<BASELINE_DIR_STATE>",
        baseline_state,
        "</BASELINE_DIR_STATE>",
        "",
        "<TOOLS_GENERATED_STATE>",
        tools_generated_state,
        "</TOOLS_GENERATED_STATE>",
        "",
        "<TASK>",
        "1. Inspect baseline/ to understand the reference evaluation harness (evaluate.sh, scripts, configs, workloads).",
        "2. Inspect output/ to understand the generated system.",
        "3. Inspect tools/generated/ for any existing eval tools from step 3.",
        "4. If a tool in tools/generated/ already matches the baseline eval pattern, reuse it.",
        "5. Otherwise, build the equivalent evaluation harness for the generated system under tools/generated/.",
        "6. The entrypoint must be tools/generated/eval.sh and it must be runnable outside the agent.",
        "7. Model eval.sh after baseline/redis/evaluate.sh — same phases, same scoring, same output structure.",
        "8. Run eval.sh and fix any compile or runtime errors until it runs successfully.",
        "9. Do NOT modify any code in output/ — only create or modify files in tools/generated/.",
        "</TASK>",
    ]
    if user_prompt:
        parts.extend(["", "<ADDITIONAL_CONTEXT>", user_prompt, "</ADDITIONAL_CONTEXT>"])
    return "\n".join(parts)


def _build_fix_prompt(
    prev_result: str,
    output_state: str,
    tools_generated_state: str,
) -> str:
    return "\n".join([
        "<PREVIOUS_RESULT>",
        prev_result,
        "</PREVIOUS_RESULT>",
        "",
        "<OUTPUT_DIR_STATE>",
        output_state,
        "</OUTPUT_DIR_STATE>",
        "",
        "<TOOLS_GENERATED_STATE>",
        tools_generated_state,
        "</TOOLS_GENERATED_STATE>",
        "",
        "<TASK>",
        "The previous attempt to build or run eval.sh had errors.",
        "Fix the issues in tools/generated/ and rerun eval.sh until it completes successfully.",
        "Do NOT modify any code in output/.",
        "</TASK>",
    ])


def _build_comparison_prompt(
    eval_result: str,
    design_spec: str,
    baseline_state: str,
) -> str:
    return "\n".join([
        "<EVAL_RESULT>",
        eval_result,
        "</EVAL_RESULT>",
        "",
        "<DESIGN_SPEC>",
        design_spec,
        "</DESIGN_SPEC>",
        "",
        "<BASELINE_DIR_STATE>",
        baseline_state,
        "</BASELINE_DIR_STATE>",
        "",
        "<TASK>",
        "eval.sh ran successfully. Now produce a clear comparison of the generated system vs the baseline.",
        "Run the baseline evaluation if results don't already exist.",
        "Use baseline/redis/scripts/compare.py if both result sets are available.",
        "Write the full comparison output (text table, metrics, verdict) to 4_evaluation/comparison.txt.",
        "The comparison.txt should include:",
        "- Throughput comparison (ops/sec)",
        "- Latency comparison (p50, p95, p99, p999) per operation type",
        "- A summary verdict on how the generated system compares to the baseline",
        "If comparison is not yet meaningful (e.g. no benchmark data), state that clearly.",
        "</TASK>",
    ])


def _check_eval_sh_success(agent_response: str) -> bool:
    lower = agent_response.lower()
    fail_signals = [
        "build failed",
        "compile error",
        "compilation error",
        "syntax error",
        "command not found",
        "no such file",
        "permission denied",
        "cannot find",
        "fatal error",
        "eval.sh failed",
        "exit code 1",
        "exit code 2",
        "runtime error",
        "panic:",
    ]
    if any(sig in lower for sig in fail_signals):
        verdict_match = re.search(r"(?:^|\n)(?:##?\s*)?VERDICT\s*\n(.*?)(?=\n(?:##?\s*)?[A-Z_]{3,}|\Z)", lower, re.DOTALL)
        if verdict_match:
            verdict = verdict_match.group(1).strip()
            if "pass" in verdict and "fail" not in verdict:
                return True
        return False
    if "eval.sh" in lower and ("pass" in lower or "success" in lower or "completed" in lower):
        return True
    verdict_match = re.search(r"(?:^|\n)(?:##?\s*)?VERDICT\s*\n(.*?)(?=\n(?:##?\s*)?[A-Z_]{3,}|\Z)", lower, re.DOTALL)
    if verdict_match:
        verdict = verdict_match.group(1).strip()
        if "pass" in verdict:
            return True
    return False


def _print_header(provider: str, max_iterations: int) -> None:
    print(f"\n{'='*60}", file=sys.stderr)
    print("  Evaluation Pipeline — Build, Verify, Compare", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"  Provider:   {provider}", file=sys.stderr)
    print(f"  Max iters:  {max_iterations}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)


def _print_iteration_header(iteration: int, max_iterations: int) -> None:
    print(f"\n{'─'*60}", file=sys.stderr)
    print(f"  Iteration {iteration} / {max_iterations}", file=sys.stderr)
    print(f"{'─'*60}\n", file=sys.stderr)


def _print_summary(
    iterations_run: int,
    eval_sh_passed: bool,
    comparison_written: bool,
    logs_path: Path,
) -> None:
    print(f"\n{'='*60}", file=sys.stderr)
    print("  Pipeline Complete", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"  Iterations:    {iterations_run}", file=sys.stderr)
    print(f"  eval.sh:       {'PASS' if eval_sh_passed else 'NOT PASSING'}", file=sys.stderr)
    print(f"  comparison:    {'written' if comparison_written else 'not written'}", file=sys.stderr)
    print(f"  Log:           {logs_path}", file=sys.stderr)
    if comparison_written:
        print(f"  Comparison:    {COMPARISON_PATH}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)


def _clean_previous_run() -> None:
    for path in [LOGS_PATH, COMPARISON_PATH]:
        if path.exists():
            path.unlink()


def run_pipeline(
    max_iterations: int = 10,
    provider: str = "claude",
    user_prompt: str | None = None,
) -> str:
    _clean_previous_run()
    design_spec = _load_file(DESIGN_SPEC_PATH, "Design spec")
    eval_system = _load_file(EVAL_AGENT_PROMPT_PATH, "Evaluation agent prompt")

    TOOLS_GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    logs: list[str] = []
    _log(logs, f"Pipeline started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    _log(logs, f"Provider:       {provider}")
    _log(logs, f"Max iterations: {max_iterations}")
    _log(logs, "=" * 60)

    _print_header(provider, max_iterations)

    eval_sh_passed = False
    comparison_written = False
    last_result = ""
    iterations_run = 0

    for iteration in range(1, max_iterations + 1):
        iterations_run = iteration
        _print_iteration_header(iteration, max_iterations)

        output_state = _list_files(OUTPUT_DIR)
        baseline_state = _list_files(BASELINE_DIR)
        tools_generated_state = _list_files(TOOLS_GENERATED_DIR)

        if iteration == 1:
            prompt = _build_initial_prompt(
                design_spec, output_state, baseline_state, tools_generated_state, user_prompt
            )
        else:
            prompt = _build_fix_prompt(last_result, output_state, tools_generated_state)

        print("  [Eval Agent] Working...", file=sys.stderr)
        spinner = _Spinner("Eval agent running").start()
        try:
            last_result = _call_eval_agent(prompt, eval_system, provider)
        except Exception as e:
            spinner.stop()
            last_result = f"EVAL AGENT ERROR: {e}"
            _log(logs, f"\n[Iteration {iteration}] ERROR: {e}")
            print(f"  [Eval Agent] Error: {e}", file=sys.stderr)
            continue
        finally:
            spinner.stop()

        _log(logs, f"\n{'='*60}")
        _log(logs, f"ITERATION {iteration} — EVAL AGENT ({provider})")
        _log(logs, f"{'='*60}")
        _log(logs, last_result)
        print(f"  [Eval Agent] Done ({len(last_result)} chars).", file=sys.stderr)

        eval_sh_passed = _check_eval_sh_success(last_result)
        print(f"  [Eval Agent] eval.sh status: {'PASS' if eval_sh_passed else 'NOT YET'}", file=sys.stderr)

        if eval_sh_passed:
            _log(logs, f"\neval.sh passed at iteration {iteration}.")
            break

    if eval_sh_passed:
        print("\n  [Comparison] Generating comparison...", file=sys.stderr)
        baseline_state = _list_files(BASELINE_DIR)
        comparison_prompt = _build_comparison_prompt(last_result, design_spec, baseline_state)

        spinner = _Spinner("Generating comparison").start()
        try:
            comparison_result = _call_eval_agent(comparison_prompt, eval_system, provider)
        except Exception as e:
            spinner.stop()
            comparison_result = f"COMPARISON ERROR: {e}"
            _log(logs, f"\nCOMPARISON ERROR: {e}")
            print(f"  [Comparison] Error: {e}", file=sys.stderr)
        else:
            spinner.stop()
            _log(logs, f"\n{'='*60}")
            _log(logs, "COMPARISON PHASE")
            _log(logs, f"{'='*60}")
            _log(logs, comparison_result)

            if COMPARISON_PATH.exists():
                comparison_written = True
                print(f"  [Comparison] Written to {COMPARISON_PATH}", file=sys.stderr)
            else:
                COMPARISON_PATH.write_text(comparison_result + "\n")
                comparison_written = True
                print(f"  [Comparison] Agent did not write file, saved response to {COMPARISON_PATH}", file=sys.stderr)
        finally:
            spinner.stop()

    _log(logs, f"\n{'='*60}")
    _log(logs, f"Pipeline finished: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    _log(logs, f"Iterations run: {iterations_run}")
    _log(logs, f"eval.sh passed: {eval_sh_passed}")
    _log(logs, f"comparison written: {comparison_written}")
    LOGS_PATH.write_text("\n".join(logs) + "\n")

    _print_summary(iterations_run, eval_sh_passed, comparison_written, LOGS_PATH)

    print(f"  Full log saved to: {LOGS_PATH}", file=sys.stderr)
    print("\nDone! Evaluation pipeline complete.\n", file=sys.stderr)

    return last_result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluation pipeline: build eval.sh, verify it runs, compare with baseline."
    )
    parser.add_argument(
        "-n",
        "--max-iterations",
        type=int,
        default=10,
        help="Maximum iterations to get eval.sh passing (default: 10)",
    )
    parser.add_argument(
        "--provider",
        choices=["claude", "openai"],
        default="claude",
        help="LLM provider for the evaluation agent (default: claude)",
    )
    parser.add_argument(
        "-p",
        "--prompt",
        type=str,
        default=None,
        help="Additional context or instructions",
    )
    parser.add_argument(
        "-f",
        "--prompt-file",
        type=Path,
        default=None,
        help="Read additional context from a file",
    )
    args = parser.parse_args()

    if args.max_iterations < 1:
        print("Error: --max-iterations must be at least 1.", file=sys.stderr)
        return 1

    user_prompt = None
    if args.prompt_file is not None:
        user_prompt = args.prompt_file.read_text().strip()
    elif args.prompt:
        user_prompt = args.prompt

    try:
        result = run_pipeline(
            max_iterations=args.max_iterations,
            provider=args.provider,
            user_prompt=user_prompt,
        )
        print(result)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
