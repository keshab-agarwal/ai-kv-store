"""Base tool definitions and registry."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ToolResult:
    success: bool
    output: str
    error: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)


class BaseTool(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def description(self) -> str: ...

    @property
    @abstractmethod
    def input_schema(self) -> dict: ...

    @abstractmethod
    async def execute(self, **kwargs) -> ToolResult: ...

    def to_anthropic_tool(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }

    def to_openai_tool(self) -> dict:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_schema,
            },
        }


class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, BaseTool] = {}

    def register(self, tool: BaseTool):
        self._tools[tool.name] = tool

    def get(self, name: str) -> BaseTool | None:
        return self._tools.get(name)

    def list_tools(self, names: list[str] | None = None) -> list[BaseTool]:
        if names:
            return [self._tools[n] for n in names if n in self._tools]
        return list(self._tools.values())

    def to_anthropic_tools(self, names: list[str] | None = None) -> list[dict]:
        return [t.to_anthropic_tool() for t in self.list_tools(names)]

    def to_openai_tools(self, names: list[str] | None = None) -> list[dict]:
        return [t.to_openai_tool() for t in self.list_tools(names)]
