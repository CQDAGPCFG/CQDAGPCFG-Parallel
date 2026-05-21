from __future__ import annotations

from time import monotonic

from .types import Lease, NodeId, WorkerId


class LeaseError(RuntimeError):
    pass


class LeaseDeniedError(LeaseError):
    pass


class StaleLeaseError(LeaseError):
    pass


class LeaseTable:
    def __init__(self, *, default_ttl_seconds: float = 30.0) -> None:
        if default_ttl_seconds <= 0.0:
            raise ValueError("default_ttl_seconds must be positive")
        self.default_ttl_seconds = default_ttl_seconds
        self._leases: dict[NodeId, dict[int, Lease]] = {}
        self._epochs: dict[NodeId, int] = {}

    def acquire(
        self,
        node_id: NodeId,
        worker_id: WorkerId,
        *,
        start: int = 0,
        end: int | None = None,
        ttl_seconds: float | None = None,
        now: float | None = None,
        allow_concurrent: bool = False,
    ) -> Lease:
        if start < 0:
            raise ValueError("lease start cannot be negative")
        if end is not None and end <= start:
            raise ValueError("lease end must be greater than start")
        now_value = monotonic() if now is None else now
        ttl = self.default_ttl_seconds if ttl_seconds is None else ttl_seconds
        if ttl <= 0.0:
            raise ValueError("ttl_seconds must be positive")

        active = self._active_leases(node_id, now=now_value)
        if active and not allow_concurrent:
            raise LeaseDeniedError("node already has an active lease")
        for lease in active:
            if lease.overlaps(start, end):
                raise LeaseDeniedError("lease range overlaps an active lease")

        epoch = self._epochs.get(node_id, 0) + 1
        self._epochs[node_id] = epoch
        lease = Lease(
            node_id=node_id,
            worker_id=worker_id,
            epoch=epoch,
            acquired_at=now_value,
            expires_at=now_value + ttl,
            start=start,
            end=end,
        )
        self._leases.setdefault(node_id, {})[epoch] = lease
        return lease

    def current(self, node_id: NodeId) -> Lease | None:
        active = self._active_leases(node_id)
        if not active:
            return None
        return max(active, key=lambda lease: lease.epoch)

    def lease_for(self, node_id: NodeId, epoch: int) -> Lease | None:
        lease = self._leases.get(node_id, {}).get(epoch)
        if lease is None or lease.is_expired(monotonic()):
            return None
        return lease

    def active_count(self, node_id: NodeId, *, now: float | None = None) -> int:
        return len(self._active_leases(node_id, now=now))

    def active_leases(
        self,
        node_id: NodeId,
        *,
        now: float | None = None,
    ) -> tuple[Lease, ...]:
        return self._active_leases(node_id, now=now)

    def contiguous_reserved_end(
        self,
        node_id: NodeId,
        base: int,
        *,
        now: float | None = None,
    ) -> int:
        if base < 0:
            raise ValueError("base cannot be negative")
        cursor = base
        for lease in sorted(
            self._active_leases(node_id, now=now),
            key=lambda active_lease: active_lease.start,
        ):
            if lease.end is None:
                continue
            if lease.start > cursor:
                break
            cursor = max(cursor, lease.end)
        return cursor

    def validate(
        self,
        *,
        node_id: NodeId,
        worker_id: WorkerId,
        epoch: int,
        start: int | None = None,
        end: int | None = None,
        now: float | None = None,
    ) -> bool:
        now_value = monotonic() if now is None else now
        lease = self._leases.get(node_id, {}).get(epoch)
        return (
            lease is not None
            and lease.worker_id == worker_id
            and lease.epoch == epoch
            and (start is None or lease.start == start)
            and (end is None or lease.end == end)
            and not lease.is_expired(now_value)
        )

    def require_valid(
        self,
        *,
        node_id: NodeId,
        worker_id: WorkerId,
        epoch: int,
        start: int | None = None,
        end: int | None = None,
        now: float | None = None,
    ) -> None:
        if not self.validate(
            node_id=node_id,
            worker_id=worker_id,
            epoch=epoch,
            start=start,
            end=end,
            now=now,
        ):
            raise StaleLeaseError("lease is missing, expired, or stale")

    def release(self, lease: Lease) -> bool:
        leases = self._leases.get(lease.node_id)
        if not leases:
            return False
        current = leases.get(lease.epoch)
        if current is None or current.worker_id != lease.worker_id:
            return False
        del leases[lease.epoch]
        if not leases:
            del self._leases[lease.node_id]
        return True

    def transfer(
        self,
        *,
        node_id: NodeId,
        source_worker_id: WorkerId,
        source_epoch: int,
        target_worker_id: WorkerId,
        ttl_seconds: float | None = None,
        now: float | None = None,
    ) -> Lease:
        """Atomically move node ownership to a target worker.

        Migration commits use this instead of release+acquire so stale source
        publishes are fenced by the next epoch before another worker starts
        writing the same node stream.
        """
        now_value = monotonic() if now is None else now
        ttl = self.default_ttl_seconds if ttl_seconds is None else ttl_seconds
        if ttl <= 0.0:
            raise ValueError("ttl_seconds must be positive")

        self.require_valid(
            node_id=node_id,
            worker_id=source_worker_id,
            epoch=source_epoch,
            now=now_value,
        )
        source_lease = self._leases[node_id][source_epoch]
        target_epoch = self._epochs.get(node_id, source_epoch) + 1
        self._epochs[node_id] = target_epoch
        lease = Lease(
            node_id=node_id,
            worker_id=target_worker_id,
            epoch=target_epoch,
            acquired_at=now_value,
            expires_at=now_value + ttl,
            start=source_lease.start,
            end=source_lease.end,
        )
        leases = self._leases.setdefault(node_id, {})
        leases.pop(source_epoch, None)
        leases[target_epoch] = lease
        return lease

    def release_expired(self, *, now: float | None = None) -> tuple[Lease, ...]:
        now_value = monotonic() if now is None else now
        expired = tuple(
            lease
            for leases in self._leases.values()
            for lease in leases.values()
            if lease.is_expired(now_value)
        )
        for lease in expired:
            leases = self._leases.get(lease.node_id)
            if leases is not None and leases.get(lease.epoch) == lease:
                del leases[lease.epoch]
                if not leases:
                    del self._leases[lease.node_id]
        return expired

    def _active_leases(
        self,
        node_id: NodeId,
        *,
        now: float | None = None,
    ) -> tuple[Lease, ...]:
        now_value = monotonic() if now is None else now
        leases = self._leases.get(node_id, {})
        return tuple(lease for lease in leases.values() if not lease.is_expired(now_value))


__all__ = [
    "LeaseDeniedError",
    "LeaseError",
    "LeaseTable",
    "StaleLeaseError",
]
