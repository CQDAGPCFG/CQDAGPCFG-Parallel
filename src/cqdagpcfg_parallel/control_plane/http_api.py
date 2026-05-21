from __future__ import annotations

from dataclasses import dataclass
from time import time

from cqdagpcfg_parallel.protocol import WorkerId


@dataclass(frozen=True, slots=True)
class WorkerRegistration:
    worker_id: WorkerId
    role: str
    joined_at: float


@dataclass(frozen=True, slots=True)
class WorkerHeartbeat:
    worker_id: WorkerId
    timestamp: float


class InProcessControlPlane:
    """Small control-plane model used before the ZeroMQ process split."""

    def __init__(self) -> None:
        self._workers: dict[WorkerId, WorkerRegistration] = {}
        self._heartbeats: dict[WorkerId, WorkerHeartbeat] = {}

    def register_worker(self, worker_id: WorkerId, *, role: str) -> WorkerRegistration:
        if not role:
            raise ValueError("worker role cannot be empty")
        registration = WorkerRegistration(worker_id=worker_id, role=role, joined_at=time())
        self._workers[worker_id] = registration
        self.heartbeat(worker_id)
        return registration

    def heartbeat(self, worker_id: WorkerId) -> WorkerHeartbeat:
        if worker_id not in self._workers:
            raise KeyError(f"unknown worker: {worker_id}")
        heartbeat = WorkerHeartbeat(worker_id=worker_id, timestamp=time())
        self._heartbeats[worker_id] = heartbeat
        return heartbeat

    def workers(self) -> tuple[WorkerRegistration, ...]:
        return tuple(self._workers.values())

    def last_heartbeat(self, worker_id: WorkerId) -> WorkerHeartbeat | None:
        return self._heartbeats.get(worker_id)


__all__ = [
    "InProcessControlPlane",
    "WorkerHeartbeat",
    "WorkerRegistration",
]
