# Bespoke KV: Automated Synthesis of Distributed KV Stores

An LLM-driven pipeline for iteratively generating, compiling, testing, and optimizing
a complete distributed in-memory key-value store tailored to a specific workload contract.
Inspired by the Bespoke OLAP paper (Wehrstein et al., 2026).

## Architecture

The pipeline orchestrator drives an LLM agent through 6 stages:

| Stage | Name | Description |
|-------|------|-------------|
| 1 | Architecture Planning | Analyze workload, design replication protocol (RF=2+witness), produce arch_plan.md |
| 2 | TLA+ Spec & Verification | **Spec-first**: formal TLA+ spec of protocol, TLC model checking, bug mode counterexample. No code yet. |
| 3a | Storage Engine | In-memory hash table + WAL, unit tests |
| 3b | Single-Node Server | Network handler + client stub, integration tests |
| 3c | Replication Protocol | **TLA+-guided**: implement protocol from verified spec, 3-node cluster tests |
| 3d | Cluster Assembly | Sharding + routing + full client, linearizability smoke test |
| 4 | Instrumentation | Tracing, metrics, YCSB binding, Porcupine harness |
| 5 | Conformance | Full linearizability check with fault injection — proves code matches spec |
| 6 | Optimization | 4 rounds of profiling and optimization with frozen TLA+ spec |

**Critical ordering**: TLA+ spec (Stage 2) is written and verified *before* any implementation
code. The verified spec becomes the formal blueprint that Stage 3c must faithfully implement.
TLA+ proves the design correct; Porcupine (Stage 5) proves the code matches the design.

The agent interacts through 4 tools: **patch**, **compile**, **shell**, **test**.

## Prerequisites

- Python 3.11+
- Go 1.21+
- Java (for TLC model checker)
- An API key for Anthropic Claude or OpenAI

## Setup

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY="sk-ant-..."
# or
export OPENAI_API_KEY="sk-..."
export LLM_PROVIDER="openai"
```

## Usage

### Run the full pipeline end-to-end
```bash
python main.py --full
```

### Run a specific stage
```bash
python main.py --stage 1          # Architecture planning only
python main.py --stage 2          # TLA+ spec & verification only
python main.py --stage 3c         # Replication protocol (TLA+-guided)
python main.py --stage 6          # Optimization loop only
```

### Resume from a snapshot
```bash
python main.py --resume stage2-tlaplus-verified   # Resume after TLA+ verification
python main.py --resume stage3-base-impl          # Resume after base implementation
```

### Autonomous mode (no human review prompts)
```bash
python main.py --full --auto
```

### Replay from cached LLM responses
```bash
python main.py --replay
```

### List available snapshots
```bash
python main.py --snapshots
```

### Use a different model
```bash
python main.py --full --provider openai --model gpt-4o
python main.py --full --provider anthropic --model claude-sonnet-4-20250514
```

## Project Structure

```
.
├── main.py                          # Pipeline orchestrator
├── config.py                        # Configuration
├── requirements.txt                 # Python dependencies
├── workload_contract.md             # Workload specification (immutable)
├── tools/
│   ├── base.py                      # Base tool class and registry
│   ├── patch_tool.py                # Unified diff / full file writer
│   ├── compile_tool.py              # Go compilation wrapper
│   ├── shell_tool.py                # Shell command executor
│   └── test_tool.py                 # Test runner (functional/bench/linearizability/tlc)
├── conversations/
│   ├── base.py                      # LLM client abstraction
│   ├── history.py                   # Conversation persistence and caching
│   ├── scripted.py                  # ScriptedConversation (stages 1-5)
│   ├── optimization.py              # OptimizationConversation (stage 6)
│   └── prompts/                     # Pre-written prompt sequences
│       ├── stage1_architecture.json
│       ├── stage2_tlaplus.json      # Spec-first: TLA+ before any code
│       ├── stage3a_storage.json
│       ├── stage3b_server.json
│       ├── stage3c_replication.json # TLA+-guided implementation
│       ├── stage3d_cluster.json
│       ├── stage4_instrumentation.json
│       ├── stage5_conformance.json
│       └── stage6_optimization.json
├── validation/
│   ├── functional.py                # CRUD correctness tests
│   ├── porcupine.py                 # Porcupine linearizability integration
│   └── fault_injection.py           # Fault injection framework
├── monitoring/
│   ├── baseline.py                  # Baseline performance tracking
│   └── regression.py                # Regression monitor with auto-rollback
├── snapshots/
│   └── git_snapshotter.py           # Git-based snapshotting
├── expert_knowledge/                # Curated optimization knowledge (injected at Stage 6 Round 3)
│   ├── kv_read_path.md
│   ├── kv_write_path.md
│   ├── replication_protocols.md
│   ├── concurrency_patterns.md
│   ├── network_optimization.md
│   └── persistence_strategies.md
└── output/                          # Generated KV store code (git repo)
    ├── .git/
    ├── kvstore/
    ├── client/
    ├── ycsb/
    ├── correctness/
    └── tlaplus/
```

## How It Works

### Conversation Management
- **ScriptedConversation** (Stages 1-5): Plays pre-written prompt sequences. Each prompt has an optional gate condition (compile_success, test_pass) that must be met before proceeding.
- **OptimizationConversation** (Stage 6): Self-steering. The agent analyzes benchmark results and decides what to optimize. Expert knowledge is injected at Round 3.

### Regression Monitoring
An external regression monitor (not controlled by the agent) tracks:
- Performance: p50/p95/p99 latency, throughput
- Correctness: Porcupine linearizability PASS/FAIL

If any p99 metric regresses by >5% or correctness fails, the monitor auto-rolls back the last patch and tells the agent to try a different approach.

### Snapshotting
Git-based snapshots in `./output/`:
- Every accepted patch creates a commit
- Stage boundaries get named tags (e.g., `stage2-tlaplus-verified`, `stage3-base-impl`, `stage5-conformance-baseline`)
- Rollback to any snapshot via `--resume <tag>`

### Optimization Branching
Stage 6 supports conversation branching (serialized, not parallel):
1. Read-path optimization branch
2. Write-path optimization branch
3. Network optimization branch

Each branch shares the codebase but has independent conversation context.

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | (required) | Anthropic API key |
| `OPENAI_API_KEY` | (required if using OpenAI) | OpenAI API key |
| `LLM_PROVIDER` | `anthropic` | LLM provider (`anthropic` or `openai`) |
| `LLM_MODEL` | `claude-sonnet-4-20250514` | Model name |
| `AUTO_MODE` | `false` | Skip human review prompts |
