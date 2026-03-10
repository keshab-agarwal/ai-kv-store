"""Pipeline configuration for Bespoke KV synthesis."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineConfig:
    output_dir: Path = Path("./output")
    build_dir: Path = Path("./output/build")

    # LLM
    llm_provider: str = "anthropic"
    llm_model: str = "claude-sonnet-4-20250514"
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    gemini_api_key: str = ""
    max_tokens: int = 16384
    temperature: float = 0.0

    # Compilation
    go_binary: str = "go"
    compile_timeout: int = 120

    # Testing
    test_functional_timeout: int = 30
    test_bench_timeout: int = 120
    test_bench_duration: int = 30
    test_linearizability_timeout: int = 600
    test_tlc_timeout: int = 300
    tlc_jar: str = "./output/tlaplus/tla2tools.jar"

    # Cluster
    cluster_nodes: int = 3
    shard_count: int = 3
    replication_factor: int = 2  # 2 full data replicas + 1 witness per shard
    base_port: int = 9000

    # Regression
    regression_threshold_pct: float = 5.0

    # Conversation
    history_dir: Path = Path("./conversation_history")
    cache_dir: Path = Path("./llm_cache")

    # Expert knowledge
    expert_knowledge_dir: Path = Path("./expert_knowledge")

    # Autonomy
    auto_mode: bool = False

    # Stage retry limits
    max_compile_retries: int = 20
    max_test_retries: int = 5

    def __post_init__(self):
        if not self.anthropic_api_key:
            self.anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not self.openai_api_key:
            self.openai_api_key = os.environ.get("OPENAI_API_KEY", "")
        if not self.gemini_api_key:
            self.gemini_api_key = os.environ.get("GOOGLE_API_KEY", "")

    def ensure_dirs(self):
        for d in [
            self.output_dir,
            self.build_dir,
            self.history_dir,
            self.cache_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            llm_provider=os.environ.get("LLM_PROVIDER", "anthropic"),
            llm_model=os.environ.get("LLM_MODEL", "claude-sonnet-4-20250514"),
            auto_mode=os.environ.get("AUTO_MODE", "").lower() in ("1", "true"),
        )
