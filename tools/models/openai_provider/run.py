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
