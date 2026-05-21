# CQDAGPCFG State Migration Snapshot Format

이 문서는 worker 간 CQDAGPCFG generation state를 넘기기 위한 snapshot 포맷을 정의한다. 목적은 worker가 특정 structure node의 뒤쪽 range를 다시 맡았을 때, 앞쪽 prefix를 처음부터 다시 warm-up하는 계산 중복을 줄이는 것이다.

## Design Rule

Snapshot은 모델 전체를 포함하지 않는다.

```text
snapshot payload:
  worker-local CQDAG block/frontier/cache state

external requirement:
  source worker와 target worker는 같은 model_fingerprint의 모델을 로드해야 함
```

즉, snapshot은 model delta가 아니라 execution state delta이다.

## Top-Level Object

포맷 버전은 `cqdagpcfg-state-snapshot/v1`이다.

```text
StateMigrationSnapshot
  format_version
  snapshot_id
  model_fingerprint
  source_worker_id
  target_worker_id
  created_at_unix_ms
  reason
  streams[]
  blocks[]
  watermarks[]
```

의미는 다음과 같다.

| Field | Meaning |
|---|---|
| `model_fingerprint` | 같은 CQDAGPCFG 모델인지 확인하는 hash/id |
| `streams` | protocol node로 노출된 structure-local stream 상태 |
| `blocks` | 해당 stream을 재개하는 데 필요한 CQDAG block DAG 상태 |
| `watermarks` | tracker/chunk/source reclaim 기준점 |

## Structure Stream State

`StructureStreamStateSnapshot`은 protocol node 하나의 local stream 상태이다.

```text
StructureStreamStateSnapshot
  node_id
  structure_index
  structure_name
  symbols
  root_signature
  max_records
  stream_base
  ready_end
  consumer_id
  guess_cache
```

`stream_base` 이전 index는 이미 reclaim/ack된 prefix이다. `ready_end`는 target worker가 이어서 생성할 수 있는 absolute local stream 위치이다.

`guess_cache`는 선택적이다. 가장 메모리 효율적인 migration은 `guess_cache`를 비우고 block result state만 넘긴다. 즉시 재발행이 필요한 경우에는 live `GuessRecord` window를 포함할 수 있다.

## Block State

`BlockStateSnapshot`은 CQDAG block 하나의 상태이다.

```text
BlockStateSnapshot
  signature
  kind
  results
  seed_result
  consumer_upto
  expected_consumers
  registered_consumers
  promotion_pins
  merged
```

`results`는 absolute-indexed live result window이다. 내부 구현의 `results_head` 같은 list offset은 wire format에 노출하지 않고, `base + entries[]` 형태로 정규화한다.

`kind`는 구현체 구분값이다.

```text
leaf
shared
single_consumer
shared_merged
local_merged
```

## Merged Block State

merged block은 frontier와 child cache를 함께 옮긴다.

```text
MergedBlockStateSnapshot
  left_signature
  right_signature
  left_consumer
  right_consumer
  left_cache
  right_cache
  frontier
  active_rows
  active_right_counts
  right_min_heap
  min_active_left
  min_active_right
  max_started_row
  initialized
  seeded_zero
  refines_since_reclaim
  reclaim_units
  next_reclaim_after
```

`frontier`는 heap의 논리 상태이다. restore 시에는 같은 ordering으로 heapify하거나 그대로 heap invariant를 만족하도록 적재해야 한다.

`left_cache`와 `right_cache`는 child result window이다. 이 cache window도 absolute index를 유지한다.

## Result Encoding

CQDAG result는 candidate string이 아니라 다음 쌍으로 저장한다.

```text
LogProbRankEntry
  log_prob
  rank_key
```

`rank_key`는 nested tuple/list 구조를 허용한다. JSON에서는 list로 저장하고, 로드 시 tuple 구조로 복원한다.

## Watermark

`NodeWatermarkSnapshot`은 migration 직후 안전한 reclaim 기준을 맞추기 위한 정보이다.

```text
NodeWatermarkSnapshot
  node_id
  ready_end
  reclaim_before
  target_end
```

target worker는 `reclaim_before` 이전 local result를 다시 보관하지 않는다.

## Migration Flow

```text
1. Tracker가 node reassignment 또는 role switch를 결정한다.
2. Tracker가 source lease epoch를 확인하고 migration ticket을 prepare한다.
3. Source worker는 해당 structure stream의 snapshot을 생성한다.
4. Snapshot은 target worker에게 전달된다.
5. Target worker는 model_fingerprint를 확인한다.
6. Target worker는 block repository와 structure stream state를 restore한다.
7. Tracker가 commit하면 target worker는 epoch e+1 lease owner가 된다.
8. Tracker는 같은 node의 다음 WorkItem을 target worker에게 배정한다.
```

이 포맷은 정확성 조건을 바꾸지 않는다. Snapshot restore가 실패하면 target worker는 기존 방식처럼 prefix를 다시 warm-up해서 같은 local stream range를 생성할 수 있어야 한다.

Wire protocol message는 다음 흐름을 사용한다.

```text
MIGRATE_PREPARE
MIGRATE_STATE
MIGRATE_INSTALL
MIGRATE_ACK
MIGRATE_COMMIT or MIGRATE_ABORT
```

`MIGRATE_COMMIT`은 `LeaseTable.transfer(node, source_epoch, target_worker)`를 호출해 epoch를 증가시킨다. 이 후 source worker의 epoch e publish는 stale message로 거부된다.

## Library API

`CQDAGStructureRecordSource`는 snapshot capture/restore API를 제공한다.

```python
snapshot = source.capture_state(
    model_fingerprint="model-sha256:...",
    source_worker_id=WorkerId("worker-a"),
    target_worker_id=WorkerId("worker-b"),
    node_ids=(NodeId("structure:8:..."),),
)

payload = snapshot.to_json()
restored = StateMigrationSnapshot.from_json(payload)

target_source.restore_state(
    restored,
    expected_model_fingerprint="model-sha256:...",
)
```

restore 이후 target worker는 snapshot의 `ready_end`부터 이어서 local stream을 생성할 수 있다.
