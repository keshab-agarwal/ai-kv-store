# Architecture Design — LLM-as-a-Judge Spec Pipeline

This tool generates N candidate architecture design specs from the storage plan
produced by `1_workload_contract`, then uses a judge LLM to pick the best one.

Think of it as a design competition: multiple LLM calls produce competing specs
from the same requirements, a separate judge LLM scores them on 10 criteria,
and the winning design is saved as `design_spec.txt`.

## Prerequisites

You need a `storage_plan.txt` in the `1_workload_contract/` folder. Generate one
first if you haven't:

```bash
python -m 1_workload_contract
```

## Setup

```bash
cp .env.example .env          # add your OPENAI_API_KEY and/or ANTHROPIC_API_KEY
pip install -r requirements.txt
```

## Usage

### Quickest way — just run it

```bash
python -m 2_architecture_design
```

This generates 3 candidate specs (default) using Claude, judges them with
Claude, and writes the winner to `design_spec.txt`.

### Control the number of candidates

```bash
# Generate 5 competing specs
python -m 2_architecture_design -n 5

# Generate just 1 spec (skips the judge phase)
python -m 2_architecture_design -n 1
```

### Choose providers independently

You can mix and match providers for the design LLM and the judge LLM:

```bash
# OpenAI generates specs, Claude judges
python -m 2_architecture_design --design-provider openai --judge-provider claude

# Claude generates specs, OpenAI judges
python -m 2_architecture_design --design-provider claude --judge-provider openai

# All OpenAI
python -m 2_architecture_design --design-provider openai --judge-provider openai
```

### Add extra context

```bash
# Inline
python -m 2_architecture_design -p "Focus on minimizing write amplification"

# From a file
python -m 2_architecture_design -f my_constraints.txt
```

## What happens during a run

1. The storage plan (`1_workload_contract/storage_plan.txt`) and the architecture
   design system prompt (`architecture_design.md`) are loaded.
2. The design LLM is called N times, each producing a candidate spec. A spinner
   shows progress with elapsed time.
3. All candidates are sent to the judge LLM along with the judge system prompt
   (`architecture_design_judge.md`) and the original storage plan.
4. The judge scores each candidate on 10 criteria (0–3 each, max 30) and picks
   a winner.
5. Two files are written:
   - **`design_spec.txt`** — the winning design spec.
   - **`log.txt`** — full log of every candidate response and the judge's
     evaluation.

## Options

| Flag                 | Default  | Description                                        |
|----------------------|----------|----------------------------------------------------|
| `-n` / `--num-specs` | `3`      | Number of candidate specs to generate (1…N)        |
| `--design-provider`  | `claude` | LLM provider for generating specs (`claude`/`openai`) |
| `--judge-provider`   | `claude` | LLM provider for the judge (`claude`/`openai`)     |
| `-p` / `--prompt`    | —        | Additional context for the design LLM              |
| `-f` / `--prompt-file` | —      | Read additional context from a file                |

## Using from Python

```python
from architecture_design.run import run_pipeline

result = run_pipeline(
    num_specs=3,
    design_provider="claude",      # or "openai"
    judge_provider="claude",       # or "openai"
    user_prompt="Focus on latency",  # optional
)
# Winner also saved to architecture_design/design_spec.txt
```

## Output files

| File              | Contents                                                    |
|-------------------|-------------------------------------------------------------|
| `design_spec.txt` | The winning architecture design spec                        |
| `log.txt`         | Full log of all candidates and the judge's evaluation       |

## How the judge works

The judge evaluates each candidate on 10 criteria from
`architecture_design_judge.md`:

1. Faithfulness to input
2. End-to-end completeness
3. Architectural clarity
4. Interface and contract quality
5. State and data modeling
6. Failure and resilience coverage
7. Operational maturity
8. Validation quality
9. Assumptions and tradeoffs
10. Creative usefulness

Each criterion is scored 0–3 (max 30 total). The judge outputs a structured
`<JUDGE_RESULT>` with the winner, scores, strengths, and weaknesses.
