# Workload Contract — Turn Your Idea Into a Storage Plan

This tool takes a rough system idea and turns it into a structured requirements
document (`storage_plan.txt`) through a conversational back-and-forth with an LLM.

Think of it as a requirements-engineering interview: the model asks you
clarifying questions, you answer, and once it has enough detail it produces a
clean spec that the next pipeline stage can consume.

## Setup

```bash
cp .env.example .env          # add your OPENAI_API_KEY and/or ANTHROPIC_API_KEY
pip install -r requirements.txt
```

## Usage

### Quickest way — just run it

```bash
python -m 1_workload_contract
```

You'll be prompted to describe your idea. Type it out, press **Enter twice**,
and the pipeline starts. The model will show a spinner while it thinks so you
know it's working.

### Pass your idea inline or from a file

```bash
# Inline
python -m 1_workload_contract "Build a distributed key-value store with Raft consensus"

# From a file
python -m 1_workload_contract -f idea.txt
```

### Choose a provider

Claude is the default. To use OpenAI instead:

```bash
python -m 1_workload_contract --provider openai "Build a real-time chat system"
```

## What happens during a run

1. Your idea is sent to the model.
2. A spinner shows while the model is thinking (with elapsed time).
3. If the model needs more info, it prints its questions clearly and waits for
   your answers. Type your response and press **Enter twice** to submit.
4. This loop repeats (up to `--max-rounds`, default 5) until the model is
   satisfied.
5. When done, two files are written:
   - **`storage_plan.txt`** — the structured requirements spec.
   - **`log.txt`** — a log of every question/answer exchange, so you can
     review what you told the model.

## Options

| Flag                | Default   | Description                                      |
|---------------------|-----------|--------------------------------------------------|
| `--provider`        | `claude`  | LLM provider (`claude` or `openai`)              |
| `--max-rounds`      | `5`       | Max Q&A rounds before stopping                   |
| `--non-interactive` | off       | Stop at the first set of questions (no prompting) |
| `-f` / `--file`     | —         | Read the system idea from a file instead of stdin |

## Using from Python

```python
from workload_contract.run import run_pipeline

result = run_pipeline(
    user_idea="Build a distributed key-value store with Raft consensus",
    provider="claude",       # or "openai"
    max_rounds=5,
    interactive=False,       # True for terminal Q&A loop
)
# Result also saved to workload_contract/storage_plan.txt
```

## Output files

| File               | Contents                                           |
|--------------------|----------------------------------------------------|
| `storage_plan.txt` | The final structured `<SYSTEM_INPUT>` spec          |
| `log.txt`      | Log of all clarification questions and your answers |
