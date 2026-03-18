import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent.parent.parent / ".env"
load_dotenv(_env_path)


def get_shell_tool():
    from agents import ShellTool
    import subprocess

    async def run_shell(request) -> str:
        commands = request.data.action.commands
        script = "\n".join(commands)
        proc = subprocess.run(
            script, shell=True, capture_output=True, text=True,
            timeout=60, encoding="utf-8", errors="replace",
        )
        output = ""
        if proc.stdout:
            output += proc.stdout
        if proc.stderr:
            output += proc.stderr
        if not output:
            output = f"exit={proc.returncode}"
        return output

    return ShellTool(executor=run_shell)


def run(
    input_text: str,
    instructions: str = "You are a helpful assistant.",
    tools: list | None = None,
    model: str = "gpt-5.4",
) -> str:
    text, _, _ = run_with_usage(input_text, instructions=instructions, tools=tools, model=model)
    return text


def run_with_usage(
    input_text: str,
    instructions: str = "You are a helpful assistant.",
    tools: list | None = None,
    model: str = "gpt-5.4",
) -> tuple[str, int, int]:
    """Returns (text, input_tokens, output_tokens) summed across all turns."""
    from agents import Agent, Runner

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    agent = Agent(name="Assistant", instructions=instructions, model=model, tools=tools or [])
    result = asyncio.run(Runner.run(agent, input_text))
    total_input = 0
    total_output = 0
    for resp in getattr(result, "raw_responses", []):
        usage = getattr(resp, "usage", None)
        if usage is not None:
            total_input += getattr(usage, "input_tokens", 0)
            total_output += getattr(usage, "output_tokens", 0)
    return result.final_output or "", total_input, total_output


async def run_async(
    input_text: str,
    instructions: str = "You are a helpful assistant.",
    tools: list | None = None,
    model: str = "gpt-4o",
) -> str:
    from agents import Agent, Runner

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    agent = Agent(name="Assistant", instructions=instructions, model=model, tools=tools or [])
    result = await Runner.run(agent, input_text)
    return result.final_output or ""
