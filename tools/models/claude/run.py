import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent.parent.parent / ".env"
load_dotenv(_env_path)


def run_agent(
    prompt: str,
    allowed_tools: list[str] | None = None,
    system_prompt: str | None = None,
    cwd: str | Path | None = None,
    max_turns: int | None = None,
) -> str:
    text, _, _ = run_agent_with_usage(
        prompt, allowed_tools=allowed_tools, system_prompt=system_prompt,
        cwd=cwd, max_turns=max_turns,
    )
    return text


def run_agent_with_usage(
    prompt: str,
    allowed_tools: list[str] | None = None,
    system_prompt: str | None = None,
    cwd: str | Path | None = None,
    max_turns: int | None = None,
) -> tuple[str, int, int]:
    """Returns (text, input_tokens, output_tokens). Token counts are best-effort."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise ValueError("ANTHROPIC_API_KEY not set")

    try:
        from claude_agent_sdk import ClaudeAgentOptions, query
    except ImportError:
        raise ImportError("pip install claude-agent-sdk") from None

    options_kw: dict = {}
    options_kw["allowed_tools"] = allowed_tools or ["Read", "Edit", "Bash"]
    if system_prompt:
        options_kw["system_prompt"] = system_prompt
    if cwd is not None:
        options_kw["cwd"] = str(cwd)
    if max_turns is not None:
        options_kw["max_turns"] = max_turns

    options = ClaudeAgentOptions(**options_kw)
    chunks: list[str] = []
    total_input = 0
    total_output = 0

    async def collect():
        nonlocal total_input, total_output
        async for message in query(prompt=prompt, options=options):
            msg_type = type(message).__name__
            if "Assistant" in msg_type and hasattr(message, "content"):
                for block in message.content:
                    if hasattr(block, "text"):
                        chunks.append(block.text)
            usage = getattr(message, "usage", None)
            if usage is not None:
                total_input += getattr(usage, "input_tokens", 0)
                total_output += getattr(usage, "output_tokens", 0)
        return "\n".join(chunks) if chunks else ""

    text = asyncio.run(collect())
    return text, total_input, total_output


def run(
    prompt: str,
    model: str = "claude-sonnet-4-20250514",
    max_tokens: int = 1024,
    system: str | None = None,
) -> str:
    text, _, _ = run_with_usage(prompt, model=model, max_tokens=max_tokens, system=system)
    return text


def run_with_usage(
    prompt: str,
    model: str = "claude-sonnet-4-20250514",
    max_tokens: int = 1024,
    system: str | None = None,
) -> tuple[str, int, int]:
    """Returns (text, input_tokens, output_tokens)."""
    from anthropic import Anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    client = Anthropic(api_key=api_key)
    kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system

    message = client.messages.create(**kwargs)
    text = ""
    for block in message.content:
        if hasattr(block, "text"):
            text = block.text
            break
    input_tokens = getattr(message.usage, "input_tokens", 0)
    output_tokens = getattr(message.usage, "output_tokens", 0)
    return text, input_tokens, output_tokens
