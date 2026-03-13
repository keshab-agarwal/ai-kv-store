# Code Generation — Manager-Engineer-Verifier Pipeline

This tool takes the `design_spec.txt` from stage 2 and iteratively builds a
working implementation via a Manager-Engineer-Verifier loop.

The Manager plans tasks, the Engineer executes them (writing code to `output/`
and `tools/generated/`), and the Verifier checks results. The loop stops when
the eval gate criteria all pass or the iteration budget is exhausted.

All generated code goes exclusively into `output/` and `tools/generated/` —
no other directories in the repo are modified.

## Prerequisites

You need a `design_spec.txt` in the `2_architecture_design/` folder. Generate
one first if you haven't:

```bash
python -m 2_architecture_design
```

## Setup

```bash
cp .env.example .env          # add your OPENAI_API_KEY and/or ANTHROPIC_API_KEY
pip install -r requirements.txt
```

## Usage

### Quickest way — just run it

```bash
python -m 3_code_gen
```

This runs up to 10 iterations (default) using Claude for all three agents.

### Interactive mode — give feedback after each iteration

```bash
python -m 3_code_gen --interactive
```

After each Manager-Engineer-Verifier cycle, the pipeline pauses and asks for
your feedback. You can:
- Type feedback to steer the next iteration (e.g. "focus on the WAL next",
  "the API server has a bug in the PUT handler")
- Press Enter with no input to continue without feedback
- Type `quit` to stop the pipeline early

Your feedback is passed to the Manager at the start of the next iteration
as a `<USER_FEEDBACK>` block and logged to `log.txt`.

### Control the iteration budget

```bash
python -m 3_code_gen -n 5
python -m 3_code_gen -n 1
```

### Choose providers independently

```bash
python -m 3_code_gen --manager-provider claude --engineer-provider openai --verifier-provider claude
python -m 3_code_gen --manager-provider openai --engineer-provider openai --verifier-provider openai
```

### Add extra context

```bash
python -m 3_code_gen -p "Focus on getting the WAL working first"
python -m 3_code_gen -f my_priorities.txt
```

## What happens during a run

1. The design spec and system prompts for all three agents are loaded.
2. Any output from a previous run is cleared (`log.txt`, `output/`,
   `tools/generated/`).
3. Each iteration runs three steps:
   - **Manager** reviews the current state, updates `output/TODO.md`, and assigns the
     next bounded task to the Engineer.
   - **Engineer** executes the task — writing code and artifacts exclusively
     under `output/` and `tools/generated/`.
   - **Verifier** checks the Engineer's work against the task scope, confirms
     all files are in the right place, and reports `EVAL_GATE_STATUS`.
4. In `--interactive` mode, the pipeline pauses for your feedback.
5. If all eval gate criteria pass, the loop stops early.
6. Otherwise, the loop continues until the iteration budget is exhausted.
7. Two files are written:
   - **`output/TODO.md`** — the living task plan, updated each iteration.
   - **`log.txt`** (in `3_code_gen/`) — full log of every agent response.

## Options

| Flag                      | Default  | Description                                       |
|---------------------------|----------|---------------------------------------------------|
| `-n` / `--max-iterations` | `10`     | Max Manager-Engineer-Verifier iterations           |
| `--manager-provider`      | `claude` | LLM provider for the Manager (`claude`/`openai`)  |
| `--engineer-provider`     | `claude` | LLM provider for the Engineer (`claude`/`openai`) |
| `--verifier-provider`     | `claude` | LLM provider for the Verifier (`claude`/`openai`) |
| `-p` / `--prompt`         | —        | Additional context for the Manager                |
| `-f` / `--prompt-file`    | —        | Read additional context from a file               |
| `--interactive`           | off      | Pause after each iteration for user feedback      |

## Output files

| File               | Contents                                              |
|--------------------|-------------------------------------------------------|
| `output/TODO.md`   | Living task plan updated by the Manager each iteration |
| `log.txt`          | Full log of all Manager, Engineer, and Verifier responses |
| `output/`          | All implementation artifacts created by the Engineer  |
| `tools/generated/` | Reusable tools created by the Engineer                |
