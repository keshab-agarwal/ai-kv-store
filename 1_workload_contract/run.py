import argparse
import itertools
import re
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_DIR = Path(__file__).resolve().parent
SYSTEM_PROMPT_PATH = CONTRACT_DIR / "workload_contract.md"
OUTPUT_PATH = CONTRACT_DIR / "storage_plan.txt"
LOGS_PATH = CONTRACT_DIR / "log.txt"
ANSWERS_INPUT_PATH = CONTRACT_DIR / "answers_input.txt"

_models_dir = REPO_ROOT / "tools" / "models"
if str(_models_dir) not in sys.path:
    sys.path.insert(0, str(_models_dir))


def _load_system_prompt() -> str:
    if not SYSTEM_PROMPT_PATH.exists():
        raise FileNotFoundError(f"System prompt not found: {SYSTEM_PROMPT_PATH}")
    return SYSTEM_PROMPT_PATH.read_text().strip()


def _has_system_input(text: str) -> bool:
    return "<SYSTEM_INPUT>" in text and "</SYSTEM_INPUT>" in text


def _extract_system_input(text: str) -> str:
    match = re.search(r"(<SYSTEM_INPUT>.*?</SYSTEM_INPUT>)", text, re.DOTALL)
    return match.group(1) if match else text


def _extract_questions(text: str) -> str | None:
    match = re.search(r"<QUESTIONS>(.*?)</QUESTIONS>", text, re.DOTALL)
    return match.group(1).strip() if match else None


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


def _call_claude(prompt: str, system: str) -> str:
    from claude import run
    return run(prompt, system=system, max_tokens=4096)


def _call_openai(prompt: str, system: str) -> str:
    from openai import run
    return run(prompt, instructions=system)


def _read_multiline() -> str:
    lines = []
    while True:
        line = input()
        if line == "":
            if lines and lines[-1] == "":
                break
            lines.append("")
        else:
            lines.append(line)
    return "\n".join(lines).strip()


def _write_questions_file(questions: str, round_num: int) -> None:
    content = (
        f"# Round {round_num} — Answer the questions below\n"
        f"# Write your answers under each question.\n"
        f"# Lines starting with # are ignored.\n"
        f"# Save this file when done, then go back to the terminal and press Enter.\n"
        f"\n{questions}\n\n"
        f"# --- Write your answers below this line ---\n\n"
    )
    ANSWERS_INPUT_PATH.write_text(content)


def _read_answers_file() -> str:
    if not ANSWERS_INPUT_PATH.exists():
        return ""
    lines = ANSWERS_INPUT_PATH.read_text().splitlines()
    marker = "# --- Write your answers below this line ---"
    answer_lines = []
    found_marker = False
    for line in lines:
        if marker in line:
            found_marker = True
            continue
        if found_marker:
            if line.startswith("#"):
                continue
            answer_lines.append(line)
    return "\n".join(answer_lines).strip()


def _collect_answers_via_file(questions: str, round_num: int) -> str:
    _write_questions_file(questions, round_num)
    print(f"  Questions written to: {ANSWERS_INPUT_PATH}", file=sys.stderr)
    print(f"  Open that file, write your answers below the marker line, save it.", file=sys.stderr)
    print(f"  Press Enter here when you're done...", file=sys.stderr)
    input()
    return _read_answers_file()


def _clean_previous_run() -> None:
    for path in [OUTPUT_PATH, LOGS_PATH, ANSWERS_INPUT_PATH]:
        if path.exists():
            path.unlink()


def run_pipeline(
    user_idea: str,
    provider: str = "claude",
    max_rounds: int = 5,
    interactive: bool = True,
) -> str:
    _clean_previous_run()
    system_prompt = _load_system_prompt()
    call_fn = _call_claude if provider == "claude" else _call_openai
    conversation = user_idea

    all_answers: list[str] = []

    for round_num in range(1, max_rounds + 1):
        print(f"\n{'='*50}", file=sys.stderr)
        print(f"  Round {round_num} of {max_rounds}  ({provider})", file=sys.stderr)
        print(f"{'='*50}\n", file=sys.stderr)

        spinner = _Spinner(f"Round {round_num} — thinking").start()
        try:
            response = call_fn(conversation, system_prompt)
        finally:
            spinner.stop()

        print("Got a response from the model.\n", file=sys.stderr)

        if _has_system_input(response):
            result = _extract_system_input(response)
            OUTPUT_PATH.write_text(result)
            if all_answers:
                LOGS_PATH.write_text("\n\n".join(all_answers) + "\n")
                print(f"  Answers saved to:      {LOGS_PATH}", file=sys.stderr)
            print(f"  Storage plan saved to: {OUTPUT_PATH}", file=sys.stderr)
            print("\nDone! Your storage plan is ready.\n", file=sys.stderr)
            return result

        questions = _extract_questions(response)
        if questions:
            print("The model needs more information before it can build your plan.\n", file=sys.stderr)
            print("--- Questions ---\n", file=sys.stderr)
            print(questions, file=sys.stderr)
            print("\n-----------------\n", file=sys.stderr)

            if not interactive:
                return response

            choice = input("Answer via (f)ile or (i)nline? [f/i]: ").strip().lower()
            if choice == "f":
                answers = _collect_answers_via_file(questions, round_num)
            else:
                print("Type your answers below (press Enter twice to submit):\n", file=sys.stderr)
                answers = _read_multiline()

            all_answers.append(
                f"--- Round {round_num} ---\n"
                f"Questions:\n{questions}\n\n"
                f"Answers:\n{answers}"
            )

            conversation = (
                f"Original idea:\n{user_idea}\n\n"
                f"Previous questions:\n{questions}\n\n"
                f"User answers:\n{answers}"
            )
        else:
            print(response, file=sys.stderr)
            return response

    print(f"\nReached the maximum of {max_rounds} rounds without a final plan.", file=sys.stderr)
    print("Try re-running with more detail in your idea, or increase --max-rounds.\n", file=sys.stderr)
    return response


def main() -> int:
    parser = argparse.ArgumentParser(description="Refine a system idea into a structured storage plan.")
    parser.add_argument("idea", nargs="?", default=None)
    parser.add_argument("-f", "--file", type=Path, default=None)
    parser.add_argument("--provider", choices=["claude", "openai"], default="claude")
    parser.add_argument("--max-rounds", type=int, default=5)
    parser.add_argument("--non-interactive", action="store_true")
    args = parser.parse_args()

    if args.file is not None:
        idea = args.file.read_text().strip()
    elif args.idea:
        idea = args.idea
    elif not sys.stdin.isatty():
        idea = sys.stdin.read().strip()
    else:
        print("\nDescribe your system idea below.", file=sys.stderr)
        print("(Press Enter twice when you're done.)\n", file=sys.stderr)
        idea = _read_multiline()

    if not idea:
        print("No idea provided.", file=sys.stderr)
        return 1

    try:
        result = run_pipeline(
            user_idea=idea,
            provider=args.provider,
            max_rounds=args.max_rounds,
            interactive=not args.non_interactive,
        )
        print(result)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
