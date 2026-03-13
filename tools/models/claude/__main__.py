"""
Run Claude from the command line. Prompt from args, a file, or stdin.

  python -m claude "Your prompt here"
  python -m claude -f prompt.txt
  echo "Your prompt" | python -m claude
  python -m claude --agent "Fix the bug in auth.py"   # Claude Code Agent (tools)

Loads ANTHROPIC_API_KEY from .env (repo root).
"""

import argparse
import sys
from pathlib import Path

# Ensure repo root is on path when run as python -m claude
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from claude.run import run, run_agent


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Claude (prompt from args, file, or stdin). Use --agent for Claude Code Agent with tools."
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default=None,
        help="Prompt text (or use -f/--file or stdin)",
    )
    parser.add_argument(
        "-f", "--file",
        type=Path,
        default=None,
        help="Read prompt from this file",
    )
    parser.add_argument(
        "--agent",
        action="store_true",
        help="Use Claude Code Agent (Read, Edit, Bash tools) instead of simple chat",
    )
    parser.add_argument(
        "--cwd",
        type=Path,
        default=None,
        help="Working directory (only for --agent)",
    )
    args = parser.parse_args()

    if args.file is not None:
        prompt = args.file.read_text().strip()
    elif args.prompt:
        prompt = args.prompt
    else:
        prompt = sys.stdin.read().strip()

    if not prompt:
        print("No prompt provided. Use: prompt as arg, -f FILE, or stdin.", file=sys.stderr)
        return 1

    try:
        if args.agent:
            out = run_agent(prompt, cwd=args.cwd)
        else:
            out = run(prompt)
        print(out)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
