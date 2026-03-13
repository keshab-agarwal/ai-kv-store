# AI Pipeline (OpenAI, Claude, Loop)

One place to call OpenAI agents, Claude, and a provider-agnostic loop. Config from `.env`.

## Setup

```bash
cp .env.example .env   # add OPENAI_API_KEY and ANTHROPIC_API_KEY
pip install -r requirements.txt
```

## Layout

- **openai/** — OpenAI agents with tools (e.g. shell). Pass tools like `get_shell_tool()`.
- **claude/** — Claude via Messages API (simple chat) or **Claude Code Agent** (tools: Read, Edit, Bash). CLI: run from terminal or pass a prompt file.
- **loop/** — Run any callable until `max_calls` or `eval_fn(result)` is True.

## Calling from other folders

**OpenAI (with optional shell tool):**

```python
from openai import run, get_shell_tool

out = run("List files in current dir", tools=[get_shell_tool()])
# or no tools:
out = run("Explain recursion", instructions="You are a teacher.")
```

**Claude (simple chat or Code Agent):**

```python
from claude import run, run_agent

out = run("What is 2+2?")   # Messages API
out = run_agent("Fix the bug in auth.py", allowed_tools=["Read", "Edit", "Bash"])  # Code Agent
```

**Claude from the command line (no typing prompt in terminal):**

```bash
# Prompt as argument
python -m claude "Explain recursion in one sentence"

# Prompt from file (e.g. prompt.txt)
python -m claude -f prompt.txt

# Prompt from stdin
echo "Summarize this repo" | python -m claude

# Claude Code Agent (coding tools)
python -m claude --agent "Find and fix the bug in src/auth.py"
python -m claude --agent -f task.txt --cwd /path/to/project
```

**Loop (provider-agnostic):**

```python
from loop import run as loop_run, default_eval
from openai import run as openai_run

# Run OpenAI 5 times or until eval says stop (default_eval never stops)
results = loop_run(
    call_fn=lambda: openai_run("Say hello"),
    max_calls=5,
    eval_fn=default_eval,  # replace with e.g. lambda r: "done" in r.lower()
)
```

All three load API keys from `.env` (repo root).
