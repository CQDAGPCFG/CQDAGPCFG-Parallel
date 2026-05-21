from __future__ import annotations

from threading import Thread
from typing import Callable, Iterable, TypeVar


T = TypeVar("T")
_MISSING = object()


def run_in_threads(tasks: Iterable[Callable[[], T]]) -> tuple[T, ...]:
    results: list[T | object] = []
    errors: list[BaseException] = []
    threads: list[Thread] = []

    for index, task in enumerate(tasks):
        results.append(_MISSING)

        def runner(task_index: int = index, task_fn: Callable[[], T] = task) -> None:
            try:
                results[task_index] = task_fn()
            except BaseException as exc:  # pragma: no cover - surfaced after join
                errors.append(exc)

        thread = Thread(target=runner, name=f"cqdagpcfg-parallel-{index}", daemon=True)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError("threaded execution failed") from errors[0]
    return tuple(result for result in results if result is not _MISSING)


__all__ = [
    "run_in_threads",
]
