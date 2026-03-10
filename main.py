#!/usr/bin/env python3
"""Bespoke KV Pipeline Orchestrator.

Drives an LLM agent through 6 stages to synthesize a distributed KV store
optimized for a specific workload contract. Inspired by Bespoke OLAP
(Wehrstein et al., 2026).

Usage:
    python main.py --full                        # Run all 6 stages
    python main.py --stage 2                     # Run specific stage
    python main.py --resume stage2-tlaplus-verified  # Resume from snapshot
    python main.py --replay                      # Replay from cache
    python main.py --full --auto                 # Autonomous (no human review)
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import PipelineConfig
from conversations.base import LLMClient, Message
from conversations.history import ConversationHistory, ResponseCache
from conversations.optimization import OptimizationConversation
from conversations.scripted import ScriptedConversation
from monitoring.baseline import BaselineTracker, BenchmarkResult
from monitoring.regression import RegressionMonitor
from snapshots.git_snapshotter import GitSnapshotter
from tools import CompileTool, PatchTool, ShellTool, TestTool, ToolRegistry

log = logging.getLogger("bespoke-kv")

STAGE_TOOLS = {
    1: ["shell"],
    2: ["patch", "shell", "test"],           # TLA+ spec: patch + shell + test --tlc (no compile)
    "3a": ["patch", "compile", "shell", "test"],
    "3b": ["patch", "compile", "shell", "test"],
    "3c": ["patch", "compile", "shell", "test"],
    "3d": ["patch", "compile", "shell", "test"],
    4: ["patch", "compile", "shell", "test"],
    5: ["shell", "test"],
    6: ["patch", "compile", "shell", "test"],
}

STAGE_PROMPTS = {
    1: "stage1_architecture.json",
    2: "stage2_tlaplus.json",
    "3a": "stage3a_storage.json",
    "3b": "stage3b_server.json",
    "3c": "stage3c_replication.json",
    "3d": "stage3d_cluster.json",
    4: "stage4_instrumentation.json",
    5: "stage5_conformance.json",
}

STAGE_SNAPSHOTS = {
    1: "stage1-architecture",
    2: "stage2-tlaplus-verified",
    "3a": "stage3a-storage",
    "3b": "stage3b-server",
    "3c": "stage3c-replication",
    "3d": "stage3-base-impl",
    4: "stage4-instrumentation",
    5: "stage5-conformance-baseline",
}

ALL_STAGES = [1, 2, "3a", "3b", "3c", "3d", 4, 5, 6]


class Pipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.config.ensure_dirs()

        self.tool_registry = ToolRegistry()
        self._register_tools()

        self.snapshotter = GitSnapshotter(config.output_dir)
        self.snapshotter.init()

        self.tracker = BaselineTracker(config.history_dir / "baseline.json")
        self.regression = RegressionMonitor(
            self.tracker, self.snapshotter, config.regression_threshold_pct
        )

        api_key_map = {
            "anthropic": config.anthropic_api_key,
            "openai": config.openai_api_key,
            "gemini": config.gemini_api_key,
        }
        self.llm = LLMClient(
            provider=config.llm_provider,
            model=config.llm_model,
            api_key=api_key_map.get(config.llm_provider, ""),
            max_tokens=config.max_tokens,
            temperature=config.temperature,
        )

        self.cache = ResponseCache(config.cache_dir)

    def _register_tools(self):
        self.tool_registry.register(PatchTool(self.config.output_dir))
        self.tool_registry.register(
            CompileTool(
                self.config.output_dir,
                self.config.go_binary,
                self.config.compile_timeout,
            )
        )
        self.tool_registry.register(ShellTool(self.config.output_dir))
        self.tool_registry.register(TestTool(self.config.output_dir, self.config))

    def _load_workload_contract(self) -> str:
        wc_path = Path("workload_contract.md")
        if not wc_path.exists():
            log.warning("workload_contract.md not found, using embedded default")
            return "(Workload contract not found — check workload_contract.md)"
        return wc_path.read_text()

    def _make_history(self, session_id: str) -> ConversationHistory:
        return ConversationHistory(self.config.history_dir, session_id)

    async def run_stage(self, stage) -> bool:
        stage_key = stage
        log.info("=" * 60)
        log.info("STAGE %s", stage_key)
        log.info("=" * 60)

        if stage_key == 6:
            return await self._run_stage6()

        prompt_file = STAGE_PROMPTS.get(stage_key)
        if not prompt_file:
            log.error("No prompt file for stage %s", stage_key)
            return False

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        session_id = f"stage{stage_key}_{ts}"
        history = self._make_history(session_id)

        if stage_key == 1:
            contract = self._load_workload_contract()
            history.add(Message(
                role="user",
                content=f"Workload Contract:\n\n{contract}",
            ))

        conv = ScriptedConversation(
            llm=self.llm,
            tool_registry=self.tool_registry,
            history=history,
            cache=self.cache,
            prompts_dir=Path("conversations/prompts"),
            auto_mode=self.config.auto_mode,
        )

        tools_for_stage = STAGE_TOOLS.get(stage_key, ["shell"])
        success = await conv.run_stage(
            prompt_file,
            available_tools=tools_for_stage,
            max_retries=self.config.max_compile_retries,
        )

        history.save_full()

        if success:
            tag = STAGE_SNAPSHOTS.get(stage_key)
            self.snapshotter.snapshot(
                f"Stage {stage_key} complete",
                tag=tag,
            )
            log.info("Stage %s PASSED (snapshot: %s)", stage_key, tag)
        else:
            log.error("Stage %s FAILED", stage_key)

        return success

    async def _run_stage6(self) -> bool:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        history = self._make_history(f"stage6_{ts}")

        opt_conv = OptimizationConversation(
            llm=self.llm,
            tool_registry=self.tool_registry,
            history=history,
            cache=self.cache,
            regression_monitor=self.regression,
            snapshotter=self.snapshotter,
            expert_knowledge_dir=self.config.expert_knowledge_dir,
            system_prompt=(
                "You are optimizing a distributed KV store for maximum performance "
                "while maintaining linearizability. The system has passed full conformance "
                "verification (Stage 5). After every code change: compile + functional test. "
                "After each round: full YCSB benchmark + linearizability check. "
                "If regression is detected, the change is auto-rolled back.\n\n"
                "FROZEN TLA+ SPEC: The TLA+ spec from Stage 2 is frozen during optimization. "
                "All optimizations must be implementation-level (data structures, networking, "
                "serialization, concurrency). IF you need a protocol-level change, you MUST: "
                "(1) update the TLA+ spec, (2) re-run TLC to confirm correctness, "
                "(3) only then modify implementation code."
            ),
        )

        success = await opt_conv.run_optimization_loop(
            available_tools=STAGE_TOOLS[6],
            enable_branching=True,
        )

        history.save_full()

        if success:
            self.snapshotter.snapshot("Stage 6 optimization complete", tag="stage6-optimized")

        return success

    async def run_full(self) -> bool:
        for stage in ALL_STAGES:
            success = await self.run_stage(stage)
            if not success:
                log.error("Pipeline halted at stage %s", stage)
                return False
        log.info("Pipeline completed successfully — all 6 stages passed")
        return True

    async def run_from(self, start_stage) -> bool:
        try:
            idx = ALL_STAGES.index(start_stage)
        except ValueError:
            log.error("Unknown stage: %s", start_stage)
            return False
        for stage in ALL_STAGES[idx:]:
            success = await self.run_stage(stage)
            if not success:
                return False
        return True

    async def resume(self, snapshot_tag: str) -> bool:
        tags = self.snapshotter.list_tags()
        if snapshot_tag not in tags:
            log.error("Snapshot tag '%s' not found. Available: %s", snapshot_tag, tags)
            return False

        self.snapshotter.rollback(snapshot_tag)
        log.info("Restored snapshot: %s", snapshot_tag)

        stage_map = {v: k for k, v in STAGE_SNAPSHOTS.items()}
        if snapshot_tag in stage_map:
            completed_stage = stage_map[snapshot_tag]
            try:
                idx = ALL_STAGES.index(completed_stage)
                next_stage = ALL_STAGES[idx + 1] if idx + 1 < len(ALL_STAGES) else None
            except ValueError:
                next_stage = None

            if next_stage:
                log.info("Resuming from stage %s", next_stage)
                return await self.run_from(next_stage)
            else:
                log.info("All stages already completed at snapshot %s", snapshot_tag)
                return True

        log.info("Custom snapshot — starting from stage 1")
        return await self.run_full()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bespoke KV Pipeline: automated synthesis of distributed KV stores",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --full                              Run all 6 stages end-to-end
  python main.py --stage 2                           Run only TLA+ spec & verification
  python main.py --stage 3c                          Run only replication protocol impl
  python main.py --stage 6                           Run only the optimization loop
  python main.py --resume stage2-tlaplus-verified    Resume after TLA+ verification
  python main.py --resume stage3-base-impl           Resume after base implementation
  python main.py --replay                            Replay from cached LLM responses
  python main.py --full --auto                       Autonomous mode (no human review)

Environment variables:
  ANTHROPIC_API_KEY    API key for Claude
  OPENAI_API_KEY       API key for OpenAI
  LLM_PROVIDER         'anthropic' or 'openai' (default: anthropic)
  LLM_MODEL            Model name (default: claude-sonnet-4-20250514)
        """,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true", help="Run all 6 stages")
    mode.add_argument(
        "--stage",
        type=str,
        help="Run a specific stage (1, 2, 3a, 3b, 3c, 3d, 4, 5, 6)",
    )
    mode.add_argument("--resume", type=str, metavar="TAG", help="Resume from a snapshot tag")
    mode.add_argument("--replay", action="store_true", help="Replay from cached LLM responses")
    mode.add_argument("--snapshots", action="store_true", help="List available snapshots")

    parser.add_argument("--auto", action="store_true", help="Autonomous mode (no human review)")
    parser.add_argument(
        "--provider",
        choices=["anthropic", "openai", "gemini"],
        default=None,
        help="LLM provider",
    )
    parser.add_argument("--model", type=str, default=None, help="LLM model name")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
        help="Output directory for generated code",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    return parser.parse_args()


def normalize_stage(s: str):
    s = s.strip().lower()
    if s in ("3a", "3b", "3c", "3d"):
        return s
    try:
        return int(s)
    except ValueError:
        return s


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)


async def main():
    args = parse_args()
    setup_logging(args.verbose)

    config = PipelineConfig.from_env()
    config.auto_mode = args.auto or config.auto_mode
    config.output_dir = Path(args.output_dir)
    config.build_dir = config.output_dir / "build"

    if args.provider:
        config.llm_provider = args.provider
    if args.model:
        config.llm_model = args.model

    if args.snapshots:
        config.ensure_dirs()
        snap = GitSnapshotter(config.output_dir)
        snap.init()
        tags = snap.list_tags()
        snapshots = snap.list_snapshots()
        print("Available snapshots:")
        for s in snapshots:
            print(f"  {s['hash']} {s['message']}")
        if tags:
            print(f"\nTags: {', '.join(tags)}")
        return

    pipeline = Pipeline(config)

    if args.full:
        success = await pipeline.run_full()
    elif args.stage:
        stage = normalize_stage(args.stage)
        success = await pipeline.run_stage(stage)
    elif args.resume:
        success = await pipeline.resume(args.resume)
    elif args.replay:
        log.info("Replay mode: using cached responses only")
        success = await pipeline.run_full()
    else:
        log.error("No mode specified")
        success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
