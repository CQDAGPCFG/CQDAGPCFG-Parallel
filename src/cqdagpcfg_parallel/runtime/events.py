from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Any


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    name: str
    timestamp: float = field(default_factory=time)
    fields: dict[str, Any] = field(default_factory=dict)


class EventLog:
    def __init__(self) -> None:
        self._events: list[RuntimeEvent] = []

    def append(self, name: str, **fields: Any) -> RuntimeEvent:
        event = RuntimeEvent(name=name, fields=dict(fields))
        self._events.append(event)
        return event

    def events(self) -> tuple[RuntimeEvent, ...]:
        return tuple(self._events)


__all__ = [
    "EventLog",
    "RuntimeEvent",
]
