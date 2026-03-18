import argparse
import itertools
import re
import shutil
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CODEGEN_DIR = Path(__file__).resolve().parent
DESIGN_SPEC_PATH = REPO_ROOT / "2_architecture_design" / "design_spec.txt"
MANAGER_PROMPT_PATH = CODEGEN_DIR / "code_gen_manager.md"
ENGINEER_PROMPT_PATH = CODEGEN_DIR / "code_gen_engineer.md"
VERIFIER_PROMPT_PATH = CODEGEN_DIR / "code_gen_verifier.md"
TODO_PATH = REPO_ROOT / "output" / "TODO.md"
LOGS_PATH = CODEGEN_DIR / "log.txt"
OUTPUT_DIR = REPO_ROOT / "output"
TOOLS_DIR = REPO_ROOT / "tools"
TOOLS_GENERATED_DIR = TOOLS_DIR / "generated"

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


def _call_manager(prompt: str, system: str, provider: str) -> tuple[str, int, int]:
    """Returns (response, input_tokens, output_tokens)."""
    if provider == "claude":
        from claude import run_with_usage
        return run_with_usage(prompt, system=system, max_tokens=16384)
    elif provider == "openai":
        from openai_provider import run_with_usage
        return run_with_usage(prompt, instructions=system)
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _call_engineer(prompt: str, system: str, provider: str) -> tuple[str, int, int]:
    """Returns (response, input_tokens, output_tokens)."""
    if provider == "claude":
        from claude import run_agent_with_usage
        return run_agent_with_usage(prompt, system_prompt=system, cwd=REPO_ROOT)
    elif provider == "openai":
        from openai_provider import run_with_usage, get_shell_tool
        return run_with_usage(prompt, instructions=system, tools=[get_shell_tool()])
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _call_verifier(prompt: str, system: str, provider: str) -> tuple[str, int, int]:
    """Returns (response, input_tokens, output_tokens)."""
    if provider == "claude":
        from claude import run_agent_with_usage
        return run_agent_with_usage(prompt, system_prompt=system, cwd=REPO_ROOT)
    elif provider == "openai":
        from openai_provider import run_with_usage, get_shell_tool
        return run_with_usage(prompt, instructions=system, tools=[get_shell_tool()])
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _build_manager_prompt_initial(
    design_spec: str, repo_state: str, user_prompt: str | None
) -> str:
    parts = [
        "<DESIGN_SPEC>",
        design_spec,
        "</DESIGN_SPEC>",
        "",
        "<REPO_STATE>",
        repo_state,
        "</REPO_STATE>",
    ]
    if user_prompt:
        parts.extend(["", "<ADDITIONAL_CONTEXT>", user_prompt, "</ADDITIONAL_CONTEXT>"])
    parts.extend([
        "",
        "This is the first iteration. No prior Engineer or Verifier results exist.",
        "Create the initial TODO_MD_UPDATE (written to output/TODO.md) and assign the first Engineer task.",
    ])
    return "\n".join(parts)


def _build_manager_prompt(
    design_spec: str,
    todo: str,
    repo_state: str,
    engineer_result: str,
    verifier_result: str,
    user_feedback: str | None = None,
) -> str:
    parts = [
        "<DESIGN_SPEC>",
        design_spec,
        "</DESIGN_SPEC>",
        "",
        "<CURRENT_TODO>",
        todo,
        "</CURRENT_TODO>",
        "",
        "<REPO_STATE>",
        repo_state,
        "</REPO_STATE>",
        "",
        "<ENGINEER_RESULT>",
        engineer_result,
        "</ENGINEER_RESULT>",
        "",
        "<VERIFIER_RESULT>",
        verifier_result,
        "</VERIFIER_RESULT>",
    ]
    if user_feedback:
        parts.extend([
            "",
            "<USER_FEEDBACK>",
            user_feedback,
            "</USER_FEEDBACK>",
        ])
    parts.extend([
        "",
        "Review the Engineer and Verifier results. Include a TODO_MD_UPDATE section (written to output/TODO.md) and assign the next task.",
    ])
    return "\n".join(parts)


def _build_engineer_prompt(task: str, todo: str) -> str:
    return "\n".join([
        "<ASSIGNED_TASK>",
        task,
        "</ASSIGNED_TASK>",
        "",
        "<CURRENT_TODO>",
        todo,
        "</CURRENT_TODO>",
        "",
        "Execute the assigned task. Follow the task scope exactly.",
    ])


def _build_verifier_prompt(task: str, engineer_result: str, todo: str) -> str:
    return "\n".join([
        "<ASSIGNED_TASK>",
        task,
        "</ASSIGNED_TASK>",
        "",
        "<ENGINEER_RESULT>",
        engineer_result,
        "</ENGINEER_RESULT>",
        "",
        "<CURRENT_TODO>",
        todo,
        "</CURRENT_TODO>",
        "",
        "Verify the Engineer's work against the assigned task. Include EVAL_GATE_STATUS.",
    ])


def _extract_section(text: str, heading: str) -> str | None:
    pattern = rf"(?:^|\n)(?:##?\s*)?{re.escape(heading)}\s*\n(.*?)(?=\n(?:##?\s*)?[A-Z_]{{3,}}|\Z)"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def _extract_next_task(manager_response: str) -> str | None:
    return _extract_section(manager_response, "NEXT_ENGINEER_TASK")


def _extract_todo_update(manager_response: str) -> str | None:
    return _extract_section(manager_response, "TODO_MD_UPDATE")


def _extract_verdict(verifier_response: str) -> str | None:
    return _extract_section(verifier_response, "VERDICT")


def _check_eval_gate(verifier_response: str) -> tuple[bool, int, int]:
    section = _extract_section(verifier_response, "EVAL_GATE_STATUS")
    if not section:
        return False, 0, 0

    lines = section.strip().splitlines()
    passed = 0
    total = 0
    for line in lines:
        line_lower = line.lower().strip()
        if not line_lower or line_lower.startswith("#"):
            continue
        if any(kw in line_lower for kw in ["pass", "fail", "not yet", "unmeasurable"]):
            total += 1
            if "pass" in line_lower and "fail" not in line_lower and "not yet" not in line_lower:
                passed += 1

    all_pass = total > 0 and passed == total
    return all_pass, passed, total


def _print_header(
    manager_provider: str,
    engineer_provider: str,
    verifier_provider: str,
    max_iterations: int,
) -> None:
    print(f"\n{'='*60}", file=sys.stderr)
    print("  Code Generation Pipeline — Manager-Engineer-Verifier", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"  Manager:    {manager_provider}", file=sys.stderr)
    print(f"  Engineer:   {engineer_provider}", file=sys.stderr)
    print(f"  Verifier:   {verifier_provider}", file=sys.stderr)
    print(f"  Max iters:  {max_iterations}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)


def _print_iteration_header(iteration: int, max_iterations: int) -> None:
    print(f"\n{'─'*60}", file=sys.stderr)
    print(f"  Iteration {iteration} / {max_iterations}", file=sys.stderr)
    print(f"{'─'*60}\n", file=sys.stderr)


# Cost per million tokens (input, output) by model prefix
_COST_PER_MTOK: list[tuple[str, float, float]] = [
    ("claude-opus",     5.00, 25.00),
    ("claude-sonnet",   3.00, 15.00),
    ("claude-haiku",    1.00,  5.00),
    ("gpt-4o",          4.00, 16.00),
    ("gpt-4",           3.00, 12.00),
    ("gpt-5",           2.50, 15.00),
]


# Map provider names to their default model string for cost lookup
_PROVIDER_MODEL = {
    "claude": "claude-sonnet",
    "openai": "gpt-5",
}


def _estimate_cost(provider: str, input_tokens: int, output_tokens: int) -> float:
    model = _PROVIDER_MODEL.get(provider, provider)
    for prefix, in_rate, out_rate in _COST_PER_MTOK:
        if prefix in model:
            return (input_tokens * in_rate + output_tokens * out_rate) / 1_000_000
    return 0.0


def _format_tokens(input_tokens: int, output_tokens: int) -> str:
    return f"{input_tokens:,} in / {output_tokens:,} out"


def _print_summary(
    iterations_run: int,
    all_pass: bool,
    passed: int,
    total: int,
    logs_path: Path,
    cost_by_role: dict[str, tuple[int, int, str]] | None = None,
) -> None:
    print(f"\n{'='*60}", file=sys.stderr)
    print("  Pipeline Complete", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"  Iterations:   {iterations_run}", file=sys.stderr)
    if total > 0:
        status = "ALL PASS" if all_pass else f"{passed}/{total} passing"
        print(f"  Eval gate:    {status}", file=sys.stderr)
    else:
        print("  Eval gate:    not yet measurable", file=sys.stderr)
    if cost_by_role:
        print(f"  ── Token usage ──────────────────────────────", file=sys.stderr)
        total_in = total_out = 0
        total_cost = 0.0
        for role, (in_tok, out_tok, provider) in cost_by_role.items():
            cost = _estimate_cost(provider, in_tok, out_tok)
            total_in += in_tok
            total_out += out_tok
            total_cost += cost
            cost_str = f"  ~${cost:.4f}" if cost > 0 else ""
            print(f"  {role:<12} {_format_tokens(in_tok, out_tok)}{cost_str}", file=sys.stderr)
        cost_str = f"  ~${total_cost:.4f}" if total_cost > 0 else ""
        print(f"  {'Total':<12} {_format_tokens(total_in, total_out)}{cost_str}", file=sys.stderr)
    print(f"  Log:          {logs_path}", file=sys.stderr)
    print(f"  TODO:         output/TODO.md", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)


def _clean_previous_run() -> None:
    if LOGS_PATH.exists():
        LOGS_PATH.unlink()
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    if TOOLS_GENERATED_DIR.exists():
        shutil.rmtree(TOOLS_GENERATED_DIR)


def _read_user_feedback() -> str | None:
    print(f"\n{'─'*60}", file=sys.stderr)
    print("  Iteration complete. Enter feedback for the next iteration.", file=sys.stderr)
    print("  (Press Enter with no input to continue without feedback,", file=sys.stderr)
    print("   or type 'quit' to stop the pipeline early.)", file=sys.stderr)
    print(f"{'─'*60}", file=sys.stderr)
    print("  > ", end="", file=sys.stderr, flush=True)
    try:
        feedback = input().strip()
    except (EOFError, KeyboardInterrupt):
        return None
    if feedback.lower() == "quit":
        return None
    return feedback if feedback else ""


def run_pipeline(
    max_iterations: int = 10,
    manager_provider: str = "claude",
    engineer_provider: str = "claude",
    verifier_provider: str = "claude",
    user_prompt: str | None = None,
    interactive: bool = False,
    resume: bool = False,
) -> str:
    if not resume:
        _clean_previous_run()
    design_spec = _load_file(DESIGN_SPEC_PATH, "Design spec")
    manager_system = _load_file(MANAGER_PROMPT_PATH, "Manager system prompt")
    engineer_system = _load_file(ENGINEER_PROMPT_PATH, "Engineer system prompt")
    verifier_system = _load_file(VERIFIER_PROMPT_PATH, "Verifier system prompt")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TOOLS_GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    logs: list[str] = []
    if resume and LOGS_PATH.exists():
        logs.append(LOGS_PATH.read_text().rstrip())
        logs.append("")
    _log(logs, f"Pipeline {'resumed' if resume else 'started'}: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    _log(logs, f"Manager provider:  {manager_provider}")
    _log(logs, f"Engineer provider: {engineer_provider}")
    _log(logs, f"Verifier provider: {verifier_provider}")
    _log(logs, f"Max iterations:    {max_iterations}")
    _log(logs, "=" * 60)

    _print_header(manager_provider, engineer_provider, verifier_provider, max_iterations)

    engineer_result = ""
    verifier_result = ""
    last_all_pass = False
    last_passed = 0
    last_total = 0
    iterations_run = 0
    pending_feedback: str | None = None
    tokens_manager = [0, 0]
    tokens_engineer = [0, 0]
    tokens_verifier = [0, 0]

    for iteration in range(1, max_iterations + 1):
        iterations_run = iteration
        _print_iteration_header(iteration, max_iterations)

        repo_state = (
            f"output/ files:\n{_list_files(OUTPUT_DIR)}\n\n"
            f"tools/generated/ files:\n{_list_files(TOOLS_GENERATED_DIR)}"
        )

        todo = TODO_PATH.read_text().strip() if TODO_PATH.exists() else "(no TODO.md yet)"

        print("  [Manager] Planning next task...", file=sys.stderr)
        if iteration == 1 and not (resume and TODO_PATH.exists()):
            combined_prompt = user_prompt
            if pending_feedback:
                combined_prompt = (combined_prompt or "") + f"\n\nUser feedback: {pending_feedback}"
            manager_prompt = _build_manager_prompt_initial(design_spec, repo_state, combined_prompt)
        else:
            manager_prompt = _build_manager_prompt(
                design_spec, todo, repo_state, engineer_result, verifier_result,
                user_feedback=pending_feedback,
            )
        pending_feedback = None

        spinner = _Spinner("Manager thinking").start()
        try:
            manager_response, m_in, m_out = _call_manager(manager_prompt, manager_system, manager_provider)
        except Exception as e:
            spinner.stop()
            _log(logs, f"\n[Iteration {iteration}] MANAGER ERROR: {e}")
            print(f"  [Manager] Error: {e}", file=sys.stderr)
            continue
        finally:
            spinner.stop()
        tokens_manager[0] += m_in
        tokens_manager[1] += m_out
        if m_in or m_out:
            print(f"  [Manager] Tokens: {_format_tokens(m_in, m_out)}", file=sys.stderr)

        _log(logs, f"\n{'='*60}")
        _log(logs, f"ITERATION {iteration} — MANAGER ({manager_provider})")
        _log(logs, f"{'='*60}")
        _log(logs, manager_response)

        task = _extract_next_task(manager_response)
        todo_update = _extract_todo_update(manager_response)

        if todo_update:
            TODO_PATH.write_text(todo_update + "\n")
            print("  [Manager] TODO.md updated.", file=sys.stderr)
        else:
            TODO_PATH.write_text(manager_response + "\n")
            print("  [Manager] TODO_MD_UPDATE not parsed — wrote full manager response to TODO.md.", file=sys.stderr)

        if not task:
            print("  [Manager] No task extracted — using full response as task.", file=sys.stderr)
            task = manager_response

        todo = TODO_PATH.read_text().strip() if TODO_PATH.exists() else todo

        print("  [Engineer] Executing task...", file=sys.stderr)
        engineer_prompt = _build_engineer_prompt(task, todo)

        spinner = _Spinner("Engineer working").start()
        try:
            engineer_result, e_in, e_out = _call_engineer(engineer_prompt, engineer_system, engineer_provider)
        except Exception as e:
            spinner.stop()
            engineer_result = f"ENGINEER ERROR: {e}"
            e_in = e_out = 0
            _log(logs, f"\n[Iteration {iteration}] ENGINEER ERROR: {e}")
            print(f"  [Engineer] Error: {e}", file=sys.stderr)
            continue
        finally:
            spinner.stop()
        tokens_engineer[0] += e_in
        tokens_engineer[1] += e_out

        _log(logs, f"\n{'─'*60}")
        _log(logs, f"ITERATION {iteration} — ENGINEER ({engineer_provider})")
        _log(logs, f"{'─'*60}")
        _log(logs, engineer_result)
        tok_str = f"  Tokens: {_format_tokens(e_in, e_out)}" if (e_in or e_out) else ""
        print(f"  [Engineer] Done ({len(engineer_result)} chars).{tok_str}", file=sys.stderr)

        print("  [Verifier] Checking results...", file=sys.stderr)
        todo = TODO_PATH.read_text().strip() if TODO_PATH.exists() else todo
        verifier_prompt = _build_verifier_prompt(task, engineer_result, todo)

        spinner = _Spinner("Verifier checking").start()
        try:
            verifier_result, v_in, v_out = _call_verifier(verifier_prompt, verifier_system, verifier_provider)
        except Exception as e:
            spinner.stop()
            verifier_result = f"VERIFIER ERROR: {e}"
            v_in = v_out = 0
            _log(logs, f"\n[Iteration {iteration}] VERIFIER ERROR: {e}")
            print(f"  [Verifier] Error: {e}", file=sys.stderr)
            continue
        finally:
            spinner.stop()
        tokens_verifier[0] += v_in
        tokens_verifier[1] += v_out
        if v_in or v_out:
            print(f"  [Verifier] Tokens: {_format_tokens(v_in, v_out)}", file=sys.stderr)

        _log(logs, f"\n{'─'*60}")
        _log(logs, f"ITERATION {iteration} — VERIFIER ({verifier_provider})")
        _log(logs, f"{'─'*60}")
        _log(logs, verifier_result)

        verdict = _extract_verdict(verifier_result)
        all_pass, passed, total = _check_eval_gate(verifier_result)
        last_all_pass, last_passed, last_total = all_pass, passed, total

        print(f"  [Verifier] Verdict: {verdict or '(not parsed)'}", file=sys.stderr)
        if total > 0:
            print(f"  [Verifier] Eval gate: {passed}/{total} criteria passing", file=sys.stderr)

        iter_in = m_in + e_in + v_in
        iter_out = m_out + e_out + v_out
        iter_cost = (
            _estimate_cost(manager_provider, m_in, m_out)
            + _estimate_cost(engineer_provider, e_in, e_out)
            + _estimate_cost(verifier_provider, v_in, v_out)
        )
        total_in = tokens_manager[0] + tokens_engineer[0] + tokens_verifier[0]
        total_out = tokens_manager[1] + tokens_engineer[1] + tokens_verifier[1]
        total_cost = (
            _estimate_cost(manager_provider, tokens_manager[0], tokens_manager[1])
            + _estimate_cost(engineer_provider, tokens_engineer[0], tokens_engineer[1])
            + _estimate_cost(verifier_provider, tokens_verifier[0], tokens_verifier[1])
        )
        iter_cost_str = f"  ~${iter_cost:.4f}" if iter_cost > 0 else ""
        total_cost_str = f"  ~${total_cost:.4f} total" if total_cost > 0 else ""
        print(
            f"  [Cost] iter: {_format_tokens(iter_in, iter_out)}{iter_cost_str}"
            f"  |  session: {_format_tokens(total_in, total_out)}{total_cost_str}",
            file=sys.stderr,
        )

        if all_pass and total > 0:
            print(
                f"\n  All {total} eval gate criteria passed! Stopping loop.",
                file=sys.stderr,
            )
            _log(logs, f"\nALL EVAL GATE CRITERIA PASSED at iteration {iteration}.")
            break

        if interactive and iteration < max_iterations:
            feedback = _read_user_feedback()
            if feedback is None:
                _log(logs, f"\nUser requested early stop at iteration {iteration}.")
                break
            if feedback:
                pending_feedback = feedback
                _log(logs, f"\n[Iteration {iteration}] USER FEEDBACK: {feedback}")

    _log(logs, f"\n{'='*60}")
    _log(logs, f"Pipeline finished: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    _log(logs, f"Iterations run: {iterations_run}")
    _log(logs, f"Tokens — Manager:  {_format_tokens(*tokens_manager)}")
    _log(logs, f"Tokens — Engineer: {_format_tokens(*tokens_engineer)}")
    _log(logs, f"Tokens — Verifier: {_format_tokens(*tokens_verifier)}")
    LOGS_PATH.write_text("\n".join(logs) + "\n")

    cost_by_role = {
        "Manager":  (tokens_manager[0],  tokens_manager[1],  manager_provider),
        "Engineer": (tokens_engineer[0], tokens_engineer[1], engineer_provider),
        "Verifier": (tokens_verifier[0], tokens_verifier[1], verifier_provider),
    }
    _print_summary(iterations_run, last_all_pass, last_passed, last_total, LOGS_PATH, cost_by_role)

    print(f"  Full log saved to: {LOGS_PATH}", file=sys.stderr)
    print("\nDone! Code generation pipeline complete.\n", file=sys.stderr)

    return verifier_result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Code generation pipeline: Manager-Engineer-Verifier loop."
    )
    parser.add_argument(
        "-n",
        "--max-iterations",
        type=int,
        default=10,
        help="Maximum number of Manager-Engineer-Verifier iterations (default: 10)",
    )
    parser.add_argument(
        "--manager-provider",
        choices=["claude", "openai"],
        default="claude",
        help="LLM provider for the Manager (default: claude)",
    )
    parser.add_argument(
        "--engineer-provider",
        choices=["claude", "openai"],
        default="claude",
        help="LLM provider for the Engineer (default: claude)",
    )
    parser.add_argument(
        "--verifier-provider",
        choices=["claude", "openai"],
        default="claude",
        help="LLM provider for the Verifier (default: claude)",
    )
    parser.add_argument(
        "-p",
        "--prompt",
        type=str,
        default=None,
        help="Additional context or instructions for the Manager",
    )
    parser.add_argument(
        "-f",
        "--prompt-file",
        type=Path,
        default=None,
        help="Read additional context from a file",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Pause after each iteration for user feedback",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue from existing output/ state without wiping it",
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
            manager_provider=args.manager_provider,
            engineer_provider=args.engineer_provider,
            verifier_provider=args.verifier_provider,
            user_prompt=user_prompt,
            interactive=args.interactive,
            resume=args.resume,
        )
        print(result)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
