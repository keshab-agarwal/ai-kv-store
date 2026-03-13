"""Loop: call a function repeatedly until max_calls or eval_fn(result) is True."""

from typing import Callable, TypeVar

T = TypeVar("T")


def default_eval(result: T) -> bool:
    """
    Filler eval: never asks to stop. Replace with your own logic.

    Example:
        def my_eval(result): return "done" in str(result).lower()
    """
    return False


def run(
    call_fn: Callable[[], T],
    max_calls: int = 10,
    eval_fn: Callable[[T], bool] | None = None,
) -> list[T]:
    """
    Call call_fn over and over. Stop when (1) max_calls reached, or (2) eval_fn(result) is True.

    Args:
        call_fn: No-arg callable that returns a result (e.g. openai.run or claude.run wrapper).
        max_calls: Hard limit on number of calls. Default 10.
        eval_fn: If it returns True for a result, stop. Default: never stop (filler).

    Returns:
        List of results from each call, in order.
    """
    eval_fn = eval_fn or default_eval
    results: list[T] = []
    for _ in range(max_calls):
        result = call_fn()
        results.append(result)
        if eval_fn(result):
            break
    return results
