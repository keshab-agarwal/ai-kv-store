# Evaluation — Build, Verify, Compare

This tool takes the generated system from `output/` and the baseline from
`baseline/`, then uses an evaluation agent to build an equivalent eval harness,
verify it compiles and runs, and produce a comparison report.

## Prerequisites

You need:
- A generated system in `output/` (from stage 3)
- A baseline in `baseline/` (e.g. `baseline/redis/`)
- Optionally, tools already generated in `tools/generated/` from stage 3

```bash
cp .env.example .env          # add your OPENAI_API_KEY and/or ANTHROPIC_API_KEY
pip install -r requirements.txt
```

## Usage

### Quickest way — just run it

```bash
python -m 4_evaluation
```

This runs the evaluation agent in a loop until `eval.sh` passes or the
iteration budget is exhausted, then generates a comparison.

### Control the iteration budget

```bash
python -m 4_evaluation -n 5
python -m 4_evaluation -n 1
```

### Choose a provider

```bash
python -m 4_evaluation --provider openai
```

### Add extra context

```bash
python -m 4_evaluation -p "Skip latency plots, focus on throughput"
python -m 4_evaluation -f my_eval_notes.txt
```

## What happens during a run

1. The eval agent inspects `baseline/` to learn the reference evaluation flow.
2. It inspects `output/` and `tools/generated/` for the generated system and
   any existing eval tools from stage 3.
3. If an existing tool matches the baseline pattern, it reuses it. Otherwise it
   builds the equivalent harness under `tools/generated/` with entrypoint
   `tools/generated/eval.sh`.
4. It runs `eval.sh` and fixes compile/runtime errors in a retry loop.
5. Once `eval.sh` passes, a comparison phase runs the baseline and generated
   system benchmarks and writes the results to `comparison.txt`.
6. Two files are written:
   - **`comparison.txt`** — the final comparison of the system vs baseline.
   - **`log.txt`** — full log of every eval agent response across all iterations.

## Options

| Flag                      | Default  | Description                                      |
|---------------------------|----------|--------------------------------------------------|
| `-n` / `--max-iterations` | `10`     | Max iterations to get eval.sh passing             |
| `--provider`              | `claude` | LLM provider (`claude`/`openai`)                 |
| `-p` / `--prompt`         | —        | Additional context for the eval agent            |
| `-f` / `--prompt-file`    | —        | Read additional context from a file              |

## Using from Python

```python
from evaluation.run import run_pipeline

result = run_pipeline(
    max_iterations=10,
    provider="claude",
    user_prompt="Focus on throughput comparison",
)
```

## Output files

| File             | Contents                                              |
|------------------|-------------------------------------------------------|
| `comparison.txt` | System vs baseline comparison (throughput, latency)   |
| `log.txt`        | Full log of all eval agent responses                  |
