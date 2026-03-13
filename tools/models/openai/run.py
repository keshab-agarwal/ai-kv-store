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
        command = getattr(request, "command", None) or getattr(request, "input", str(request))
        proc = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=60)
        return proc.stdout or proc.stderr or f"exit={proc.returncode}"

    return ShellTool(executor=run_shell)


def run(
    input_text: str,
    instructions: str = "You are a helpful assistant.",
    tools: list | None = None,
    model: str = "gpt-5.4",
) -> str:
    from agents import Agent, Runner

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    agent = Agent(name="Assistant", instructions=instructions, model=model, tools=tools or [])
    result = asyncio.run(Runner.run(agent, input_text))
    return result.final_output or ""


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
