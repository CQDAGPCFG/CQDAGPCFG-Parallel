from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from uuid import uuid4

from cqdagpcfg_parallel.protocol import Lease, LeaseTable, NodeId, WorkerId


class MigrationStatus(str, Enum):
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass(frozen=True, slots=True)
class MigrationTicket:
    migration_id: str
    node_id: NodeId
    source_worker_id: WorkerId
    target_worker_id: WorkerId
    source_epoch: int
    model_fingerprint: str | None = None
    status: MigrationStatus = MigrationStatus.PREPARED
    snapshot_digest: str | None = None
    snapshot_bytes: int = 0
    reason: str = "rebalance"


@dataclass(frozen=True, slots=True)
class MigrationCommit:
    ticket: MigrationTicket
    target_lease: Lease


@dataclass(frozen=True, slots=True)
class SnapshotPolicy:
    max_snapshot_bytes: int = 8 * 1024 * 1024
    max_snapshot_to_warmup_ratio: float = 0.75

    def __post_init__(self) -> None:
        if self.max_snapshot_bytes <= 0:
            raise ValueError("max_snapshot_bytes must be positive")
        if self.max_snapshot_to_warmup_ratio < 0.0:
            raise ValueError("max_snapshot_to_warmup_ratio cannot be negative")

    def should_migrate(
        self,
        *,
        snapshot_bytes: int,
        estimated_warmup_bytes: int,
        node_priority: float = 1.0,
    ) -> bool:
        if snapshot_bytes < 0:
            raise ValueError("snapshot_bytes cannot be negative")
        if estimated_warmup_bytes < 0:
            raise ValueError("estimated_warmup_bytes cannot be negative")
        if node_priority < 0.0:
            raise ValueError("node_priority cannot be negative")
        if snapshot_bytes > self.max_snapshot_bytes:
            return False
        if estimated_warmup_bytes == 0:
            return snapshot_bytes == 0
        priority_discount = 1.0 + node_priority
        ratio = snapshot_bytes / max(estimated_warmup_bytes * priority_discount, 1)
        return ratio <= self.max_snapshot_to_warmup_ratio


@dataclass(frozen=True, slots=True)
class MigrationStats:
    prepared: int = 0
    committed: int = 0
    aborted: int = 0
    rejected: int = 0


class MigrationCoordinator:
    """Tracker-side migration guard for CQDAG node ownership.

    This class deliberately does not move bytes. It owns the small protocol
    state that decides whether a snapshot handoff can commit and delegates the
    actual owner change to ``LeaseTable.transfer``.
    """

    def __init__(self, leases: LeaseTable, *, snapshot_policy: SnapshotPolicy | None = None) -> None:
        self.leases = leases
        self.snapshot_policy = SnapshotPolicy() if snapshot_policy is None else snapshot_policy
        self._tickets: dict[str, MigrationTicket] = {}
        self._prepared = 0
        self._committed = 0
        self._aborted = 0
        self._rejected = 0

    @property
    def stats(self) -> MigrationStats:
        return MigrationStats(
            prepared=self._prepared,
            committed=self._committed,
            aborted=self._aborted,
            rejected=self._rejected,
        )

    def prepare(
        self,
        *,
        node_id: NodeId,
        source_worker_id: WorkerId,
        target_worker_id: WorkerId,
        source_epoch: int,
        model_fingerprint: str | None = None,
        reason: str = "rebalance",
    ) -> MigrationTicket:
        if self.leases.active_count(node_id) > 1:
            self._rejected += 1
            raise RuntimeError("cannot migrate node while concurrent range leases are active")
        self.leases.require_valid(
            node_id=node_id,
            worker_id=source_worker_id,
            epoch=source_epoch,
        )
        ticket = MigrationTicket(
            migration_id=str(uuid4()),
            node_id=node_id,
            source_worker_id=source_worker_id,
            target_worker_id=target_worker_id,
            source_epoch=source_epoch,
            model_fingerprint=model_fingerprint,
            reason=reason,
        )
        self._tickets[ticket.migration_id] = ticket
        self._prepared += 1
        return ticket

    def attach_snapshot(
        self,
        migration_id: str,
        *,
        snapshot_payload: str,
        snapshot_digest: str | None = None,
    ) -> MigrationTicket:
        ticket = self._require_ticket(migration_id, status=MigrationStatus.PREPARED)
        digest = snapshot_digest or content_digest(snapshot_payload)
        snapshot_bytes = len(snapshot_payload.encode("utf-8"))
        updated = MigrationTicket(
            migration_id=ticket.migration_id,
            node_id=ticket.node_id,
            source_worker_id=ticket.source_worker_id,
            target_worker_id=ticket.target_worker_id,
            source_epoch=ticket.source_epoch,
            model_fingerprint=ticket.model_fingerprint,
            status=ticket.status,
            snapshot_digest=digest,
            snapshot_bytes=snapshot_bytes,
            reason=ticket.reason,
        )
        self._tickets[migration_id] = updated
        return updated

    def commit(
        self,
        migration_id: str,
        *,
        ttl_seconds: float | None = None,
    ) -> MigrationCommit:
        ticket = self._require_ticket(migration_id, status=MigrationStatus.PREPARED)
        target_lease = self.leases.transfer(
            node_id=ticket.node_id,
            source_worker_id=ticket.source_worker_id,
            source_epoch=ticket.source_epoch,
            target_worker_id=ticket.target_worker_id,
            ttl_seconds=ttl_seconds,
        )
        committed = MigrationTicket(
            migration_id=ticket.migration_id,
            node_id=ticket.node_id,
            source_worker_id=ticket.source_worker_id,
            target_worker_id=ticket.target_worker_id,
            source_epoch=ticket.source_epoch,
            model_fingerprint=ticket.model_fingerprint,
            status=MigrationStatus.COMMITTED,
            snapshot_digest=ticket.snapshot_digest,
            snapshot_bytes=ticket.snapshot_bytes,
            reason=ticket.reason,
        )
        self._tickets[migration_id] = committed
        self._committed += 1
        return MigrationCommit(ticket=committed, target_lease=target_lease)

    def abort(self, migration_id: str) -> MigrationTicket:
        ticket = self._require_ticket(migration_id, status=MigrationStatus.PREPARED)
        aborted = MigrationTicket(
            migration_id=ticket.migration_id,
            node_id=ticket.node_id,
            source_worker_id=ticket.source_worker_id,
            target_worker_id=ticket.target_worker_id,
            source_epoch=ticket.source_epoch,
            model_fingerprint=ticket.model_fingerprint,
            status=MigrationStatus.ABORTED,
            snapshot_digest=ticket.snapshot_digest,
            snapshot_bytes=ticket.snapshot_bytes,
            reason=ticket.reason,
        )
        self._tickets[migration_id] = aborted
        self._aborted += 1
        return aborted

    def _require_ticket(
        self,
        migration_id: str,
        *,
        status: MigrationStatus,
    ) -> MigrationTicket:
        ticket = self._tickets.get(migration_id)
        if ticket is None:
            self._rejected += 1
            raise KeyError(f"unknown migration_id: {migration_id}")
        if ticket.status != status:
            self._rejected += 1
            raise RuntimeError(
                f"migration {migration_id} is {ticket.status.value}, expected {status.value}"
            )
        return ticket


def content_digest(payload: str | bytes) -> str:
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    return sha256(data).hexdigest()


__all__ = [
    "MigrationCommit",
    "MigrationCoordinator",
    "MigrationStats",
    "MigrationStatus",
    "MigrationTicket",
    "SnapshotPolicy",
    "content_digest",
]
