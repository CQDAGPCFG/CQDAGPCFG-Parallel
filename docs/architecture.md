# CQDAGPCFG-Parallel Architecture

이 패키지는 `CQDAGPCFG`를 수정하지 않고 외부에서 병렬 프로토콜을 쌓기 위한 실험 공간이다.

## Boundary

- `adapters/cqdagpcfg`: `CQDAGPCFG`의 serial oracle, root/structure record source, scheduling feature adapter를 감싸는 경계
- `protocol`: node state, demand, lease, chunk store, scheduler 같은 순수 protocol state machine
- `simulation`: deterministic single-process simulator와 global merger
- `runtime`: candidate batch, bounded queue, transport decorator, local/mock executor
- `distributed`: ZeroMQ ROUTER/DEALER 기반 tracker/worker distributed executor
- `storage`: checkpoint, chunk manifest, model manifest, state migration snapshot 자료형

## Protocol Modules

- `types.py`: `NodeId`, `Demand`, `Lease`, `WorkItem`, `EnumerationChunk`
- `chunk_store.py`: node local stream의 contiguous prefix materialization
- `node_state.py`: `ready_end`, `target_end`, priority, cost, runtime feedback 상태
- `lease_table.py`: node별 단일 writer와 epoch 검증
- `scheduler.py`: priority/cost-aware score, priority donation, runtime feedback 기반 chunk size 정책

## Runtime Modules

- `candidate_batch.py`: GPU/consumer로 넘길 후보 batch 자료형
- `batching.py`: `GuessRecord` stream을 bounded `CandidateBatch`로 자르는 로직
- `candidate_queue.py`: 메모리 상한을 지키는 producer-consumer queue
- `batch_ledger.py`: consumer failure/retry를 위한 `CandidateBatch` rank-range ledger
- `batch_transport.py`: sink/source protocol과 bounded sink decorator
- `zmq_transport.py`: `cqpcfg://` public URI를 ZeroMQ endpoint로 연결하는 transport
- `worker.py`: lease를 가진 `WorkItem`을 실행해 `EnumerationChunk`를 publish하는 local worker
- `mock_pipeline.py`: 외부 consumer 연결 전 단계의 local candidate pipeline simulator
- `candidate_pipeline.py`: 기존 import 호환을 위한 facade

## External Consumer Boundary

이 패키지는 비밀번호 검증 도구의 설정이나 실행 방식을 제공하지 않는다. 라이브러리 경계는 `CandidateBatchSink.publish(batch)`까지이며, 사용자는 필요한 도구나 검증 로직을 직접 sink로 구현한다.

## Distributed Modules

- `annotations.py`: `@cqpcfg_generator`, `@cqpcfg_consumer`, `@cqpcfg_distributed`, `@cqpcfg_tracker`, `@cqpcfg_worker` 기반 public API
- `messages.py`: `ready`, `work`, `chunk`, `exhausted`, `wait`, `stop`, `migrate_*` control message codec
- `migration.py`: snapshot handoff의 prepare/commit/abort와 lease epoch fencing
- `experiments/cqpcfg_experiment.py tracker`: demand, lease, chunk store, merger를 소유하는 ZeroMQ tracker
- `worker.py`: source에서 local result range를 materialize하는 ZeroMQ worker
- `runner.py`: in-process context로 tracker와 여러 worker를 띄우는 테스트/실험 runner
- `role_allocator.py`: CQDAG frontier/reclaim/page locality를 반영해 generator/consumer node 비율을 조정하는 elastic allocator
- `role_control.py`: persistent node agent에게 현재 role을 전달하는 ZeroMQ role control plane
- `experiments/cqpcfg_experiment.py worker`: 한 프로세스 안에서 generator/consumer role을 hot-swap하는 reusable node runtime

`NodeAgent`, `RoleController`, CQDAG source bootstrap, `@cqdagpcfg_node_agent`, `@cqdagpcfg_tracker`는 라이브러리 레벨 API이다. CLI/env parsing과 role-signal metrics reader는 `experiments/src`에서 담당한다.

## CQDAG-Aware Elastic Role Allocation

라이브러리는 외부 검증 도구를 직접 실행하지 않지만, generator와 consumer의 node 비율은 프로토콜 정책으로 제공한다. 기본 정책은 `CqdagAwareElasticRoleAllocator`이다. 이 정책은 단순 producer-consumer queue balancing이 아니라 CQDAGPCFG의 frontier, page/cache locality, reclaim pressure를 같이 본다.

기본 처리량 모델은 동일한 성능의 node가 `N`개 있고, `k`개를 generator로 둘 때 다음과 같다.

```text
T(k) = min(k * generator_rate, (N - k) * consumer_rate)
```

`ThroughputOptimalRoleAllocator`는 이 식의 유효한 정수 `k` 중 `T(k)`를 최대화하는 baseline이다. `CqdagAwareElasticRoleAllocator`는 여기에 CQDAGPCFG 신호를 추가한다.

```text
frontier pressure:
  high-priority CQDAG frontier가 충분히 materialize되지 못하는 정도
  높을수록 generator를 늘린다.

priority pressure:
  아직 높은 확률 prefix 구간을 통과 중인지 나타내는 압력
  frontier pressure와 결합해 generator 쪽 가중치를 키운다.

reclaim pressure:
  CandidateBatch, ChunkStore, source cache가 쌓여 reclaim이 필요한 정도
  높을수록 consumer를 늘려 drain/reclaim을 앞당긴다.

page/cache locality:
  현재 generator가 이미 CQDAG page/block/cache를 들고 있는 정도
  높을수록 warm generator를 consumer로 바꾸는 비용을 크게 본다.
```

내부적으로 allocator는 frontier가 막힌 경우 generator capacity를 더 희소한 자원처럼 보고, reclaim pressure가 높은 경우 consumer capacity를 더 희소한 자원처럼 본다. 따라서 같은 raw throughput이라도 CQDAG frontier를 풀거나 memory reclaim을 앞당기는 배치를 선택한다.

동적 실행에서는 `RoleAllocationInput`이 현재 generator 개수, pending candidate 수, queue capacity, generator/consumer idle ratio, role swap 비용, CQDAG frontier/reclaim/page locality 신호를 함께 받는다. Queue pressure와 reclaim pressure가 높으면 consumer를 더 두는 방향으로, queue가 비고 consumer가 놀면서 frontier pressure가 높으면 generator를 더 두는 방향으로 score를 보정한다. 단, 현재 role 배치에서 다른 배치로 옮기는 비용이 개선 효과보다 크면 기존 배치를 유지한다.

role switch 대상도 CQDAG-aware하게 고른다. generator를 consumer로 바꿔야 할 때는 page/cache가 덜 warm한 generator를 먼저 바꾸고, consumer를 generator로 바꿔야 할 때는 idle 시간이 큰 consumer를 먼저 바꾼다. 이 때문에 동적 역할 조정은 단순히 node 수만 바꾸는 것이 아니라 CQDAGPCFG 상태 locality를 보존하는 elastic resource allocation이 된다.

## Framework API

한 프로세스에서 tracker와 여러 worker를 같이 띄우는 실험은 다음처럼 쓴다. 기본 chunk/scheduling 정책은 자동으로 선택된다.

```python
@cqpcfg_distributed(limit=80, worker_count=3)
@cqpcfg_generator
def source(worker_id):
    return CQDAGRecordSource(model, max_records=96)

result, workers = source.run()
```

외부 검증 도구로 넘길 consumer는 `CandidateBatchSink` 역할만 구현한다.

```python
@cqpcfg_consumer
def consume(batch):
    external_tool.write(batch.guesses)
```

tracker와 generator-only worker를 별도 스크립트로 분리할 때는 다음처럼 선언한다.

```python
@cqpcfg_tracker(bind="cqpcfg://0.0.0.0:5555", limit=80, expected_workers=3)
def tracker_config():
    return None

tracker_config.run()
```

```python
@cqpcfg_worker(
    connect="cqpcfg://127.0.0.1:5555",
    worker_id="worker-0",
    resource_cpus=2.0,
    resource_memory="4g",
    resource_gpus=1,
    model_json_page_cache=64,
)
@cqpcfg_generator
def worker_source(worker_id):
    return CQDAGRecordSource(model, max_records=96)

worker_source.run()
```

자원 선언은 worker 쪽 decorator가 갖는다. Tracker는 worker가 보고한 CPU, memory, GPU, model page cache 정보를 role allocation 입력으로만 사용한다.

생성과 소비를 모두 맡을 수 있는 elastic node는 `@cqpcfg_node_agent`를 클래스에 붙여 선언한다. 이 경우 하나의 process가 generator와 consumer 구현을 모두 들고 있고, `RoleController`가 내려준 현재 role에 따라 하나만 실행한다.

```python
@cqpcfg_node_agent(
    connect="cqpcfg://127.0.0.1:5555",
    node_id="node-0",
    resource_cpus=2.0,
    resource_memory="4g",
    resource_gpus=1,
    model_json_page_cache=64,
)
class WorkerNode:
    @cqpcfg_generator
    def source(self, worker_id):
        return CQDAGRecordSource(model, max_records=96)

    @cqpcfg_consumer
    def consume(self, batch):
        hash_engine.verify(batch.guesses)

WorkerNode.run()
```

PCFG framework 표면은 Ray actor와 비슷한 사용감을 따른다. `@cqdagpcfg.remote(...)`는 node class를 원격 실행 가능한 PCFG actor로 선언하고, 실행은 `Node.remote()`로 시작한다. 사용자는 transport, batch id, ack, rank 보강을 직접 다루지 않는다.

```python
from hashlib import sha256

from cqdagpcfg_parallel import cqdagpcfg


@cqdagpcfg.remote(
    env_prefix="CQPCFG",
    connect="cqpcfg://tracker:5555",
    num_cpus=2,
    memory="4g",
    num_gpus=1,
    model_json_page_cache=128,
)
class ExperimentNode:
    def consume(self, guess: str):
        return sha256(guess.encode()).hexdigest()

ExperimentNode.remote()
```

`@cqdagpcfg.remote`는 Ray처럼 괄호 없이도 쓸 수 있고, resource option이 필요하면 `@cqdagpcfg.remote(...)`로 쓴다. `connect`, CPU/GPU, memory, model page cache처럼 배포자가 직접 판단해야 하는 값은 decorator에 드러내고, `env_prefix="CQPCFG"`를 주면 같은 값들을 환경 변수로 덮어쓸 수 있다. 일반 사용자는 transport subchannel, drain timeout, ack, bootstrap 같은 protocol detail을 직접 선언하지 않는다. `consume()`은 기본적으로 후보 문자열 하나를 받고, `None`, `False`, `True`, digest 문자열, `dict`, `list[dict]`를 반환할 수 있다. Tracker는 target hash table을 제공하지만, guess를 어떤 hash 알고리즘으로 digest할지는 consumer가 결정한다. 프레임워크는 반환된 digest나 `hash` 필드를 target table에 매칭하고, `rank`, `batch_id`, `guess`, `node_id`, `elapsed_seconds` 같은 실행 메타데이터를 자동으로 붙여 hit log와 metrics에 반영한다. 내부 batch를 직접 보고 싶은 고급 사용자는 parameter 이름을 `batch`로 두면 `CandidateBatch`를 그대로 받을 수 있다.

`generate()`는 선택적 hook이다. 생략하면 프레임워크가 기본 CQDAGPCFG source를 사용한다. 사용자가 후보를 필터링하거나 변형하고 싶을 때는 `guess` 또는 `record`를 받아 `None`, 문자열, `GuessRecord`, 또는 이들의 iterable을 반환한다. `None`은 해당 후보를 제거한다. 프레임워크는 반환값을 range-preserving local stream으로 감싸기 때문에 사용자가 직접 `[start,end)` source를 구현하지 않는다.

```python
class RuleNode:
    def generate(self, guess: str):
        if len(guess) < 8:
            return None
        return [guess, f"{guess}2026"]

    def consume(self, guess: str):
        return sha256(guess.encode()).hexdigest()
```

Tracker actor는 후보를 직접 변형하지 않는다. 대신 `on_start(job)`과 `on_complete(summary)` hook으로 실험 단위 metadata와 protocol summary를 받는다. 여기서 serial digest 검증, peak resident record, reclaim count, affinity hit/miss 같은 논문용 지표를 기록할 수 있다.

```python
from cqdagpcfg_parallel.adapters.cqdagpcfg import cqdagpcfg_tracker


@cqdagpcfg_tracker(config)
class ExperimentTracker:
    def on_start(self, job):
        print(job.limit, job.source_mode)

    def on_node_join(self, node):
        print(node.node_id, node.role)

    def on_node_leave(self, node):
        print(node.node_id, node.reason)

    def on_role_change(self, event):
        print(event.node_id, event.previous_role, event.new_role)

    def on_memory_snapshot(self, snapshot):
        print(snapshot.peak_resident_records, snapshot.reclaimed_records)

    def on_checkpoint(self, checkpoint):
        print(checkpoint.emitted_count)

    def on_batch_retry(self, event):
        print(event.batch_id, event.reason)

    def on_error(self, error):
        print(error.stage, error.message)

    def on_complete(self, summary):
        assert summary.digest == summary.serial_digest
        print(summary.peak_resident_records, summary.reclaimed_records)

ExperimentTracker.run()
```

기본 tracker hook은 `on_start`, `on_node_join`, `on_node_leave`, `on_role_change`, `on_memory_snapshot`, `on_checkpoint`, `on_batch_retry`, `on_error`, `on_complete`이다. 이 hook들은 user-facing 실험 이벤트만 다루며, lease epoch, chunk publish, scheduler score 같은 내부 프로토콜 객체는 직접 노출하지 않는다.

Framework service는 같은 이벤트를 `cqdagpcfg.*` logger에도 남긴다. 기본 로그는 key=value text이고, `CQPCFG_LOG_FORMAT=json`을 주면 JSON line으로 바뀐다.

```bash
CQPCFG_LOG_LEVEL=INFO \
CQPCFG_LOG_FORMAT=json \
python experiments/cqpcfg_experiment.py worker
```

대표 이벤트 이름은 다음과 같다.

- `tracker.start`, `tracker.protocol_ready`, `tracker.complete`
- `tracker.node_join`, `tracker.node_leave`, `tracker.role_change`
- `tracker.memory_snapshot`, `tracker.checkpoint`, `tracker.batch_retry`
- `node.start`, `node.job_context_received`, `node.complete`
- `node_agent.generator_session_start`, `node_agent.consumer_session_start`, `node_agent.role_switch`

## M1~M3 Flow

```text
RootShard ReadChunk miss
-> NodeStateTable register demand
-> PriorityCostScheduler select node and chunk size
-> LeaseTable acquire epoch
-> LocalProtocolWorker materialize [start,end)
-> InMemoryChunkStore publish EnumerationChunk
-> GlobalMerger emits the next serial-equivalent GuessRecord
```

사용자는 chunk policy를 고르지 않는다. 프로토콜 기본값은 `cqdag_adaptive` 하나이며, scheduler가 demand gap, node dispersion, runtime feedback, affinity, migration penalty를 함께 반영한다. `fixed`, `gap_adaptive`, `entropy_adaptive`는 논문 비교군이나 ablation 실험을 위한 low-level enum으로만 남긴다.

Scheduler score는 다음 직관을 따른다.

```text
score(node) =
  demand_gap
  * urgency
  * (priority + donated_priority)
  / effective_cost
```

`effective_cost`에는 static cost estimate와 runtime feedback이 반영된다. chunk latency가 목표보다 길거나 child miss가 많으면 해당 node의 score와 chunk size가 줄어든다. parent-child dependency가 등록된 경우, 막힌 parent의 priority는 child에게 donation되어 parent를 풀 수 있는 child work가 먼저 배정된다.

Scheduler는 soft node affinity도 적용한다. 어떤 worker가 특정 structure node를 처리한 적이 있으면, 같은 worker가 다시 work를 요청할 때 그 node의 score에 `1 + node_affinity_bonus`를 곱한다. 이 정책은 CQDAG block/cache가 이미 warm-up된 worker에게 이어진 range를 주기 위한 것이며, 보너스 방식이라서 더 중요한 node가 있으면 여전히 우선 배정될 수 있다.

다른 worker가 이미 처리하던 node를 가져가는 경우에는 `node_migration_penalty`를 score denominator에 반영할 수 있다. 이 값은 state migration이 아직 이뤄지지 않은 cold takeover를 줄이기 위한 항이다. 따라서 scheduler는 priority/cost/runtime feedback뿐 아니라 worker-local CQDAG state의 재사용 가능성도 고려한다.

## Model Fingerprint

분산 worker는 같은 CQDAGPCFG 모델을 해석해야 한다. Tracker가 `model_fingerprint`를 가진 경우, worker의 `ready/chunk/exhausted/retire` message는 같은 fingerprint를 보고해야 한다. 다르면 tracker는 해당 message를 거부한다.

`ModelManifest`는 모델 payload의 canonical JSON hash를 `sha256:<digest>` 형태로 만든다. Snapshot migration도 같은 fingerprint 위에서만 유효하다.

## CQDAGPCFG Structure Adapter

`CQDAGBlockGraphAdapter`는 학습된 CQDAGPCFG structure를 protocol node descriptor로 변환한다.

```text
node priority       = structure.base_prob
node estimated_cost = sum(1 + slot_access_weight(symbol))
node dispersion     = normalized slot-table dispersion
```

`CQDAGStructureRecordSource`는 structure별 local stream을 제공한다. 따라서 tracker는 structure node들을 독립적으로 lease할 수 있고, `GlobalMerger`가 structure head들을 합쳐 serial CQDAGPCFG prefix와 같은 후보열을 발행한다.

## Distributed Correctness Rule

기본 distributed runner는 정확한 serial tie-order 보존을 위해 single root shard로 실행한다. 여러 root shard를 쓰는 모드는 `root_shard_count`를 명시해서 켜며, shard 간 동일 `GuessRecord.order_key()` tie가 없거나 tie-order를 별도로 보존할 수 있는 실험에서 사용한다.

## Rule

병렬 scheduling이나 worker 수가 바뀌어도 출력 의미론은 `CQDAGPCFG.OptimizedCQDAGEnumerator`의 prefix와 같아야 한다.

## Memory Boundary

Tracker는 기본적으로 emitted `GuessRecord` 전체를 보관하지 않는다. 각 record는 incremental digest와 optional callback으로 흘려보내며, 디버깅이나 semantic test가 필요할 때만 `collect_outputs=True`로 output tuple을 수집한다.

`ChunkStore`는 absolute index를 유지하는 base offset을 사용한다. Merger가 이미 소비한 local prefix는 `reclaim_before(node, cursor)`로 제거할 수 있으며, 이때 `ready_end`는 뒤로 움직이지 않는다.

Generator source도 같은 방향으로 reclaim watermark를 받는다. `LocalResultSource`가 `reclaim_before(node, index)`를 구현하면 local/distributed worker는 chunk materialization 후 해당 hook을 호출한다. `CQDAGStructureRecordSource`는 이 hook에서 structure-local cache를 버리고 CQDAGPCFG block consumer에 `ack()`를 전달한다. 따라서 protocol-level chunk reclaim과 CQDAG 내부 DAG-aware reclaim이 같은 prefix watermark를 기준으로 연결된다.

worker가 이미 지난 뒤쪽 range를 직접 맡는 경우에는 요청 구간 이전 prefix를 `GuessRecord`로 cache하지 않고 작은 window 단위로 CQDAG block을 advance/ack 한다. 이 경로는 메모리를 줄이기 위한 skip-reclaim이다. 계산 중복은 soft node affinity로 먼저 줄이고, worker가 이탈하거나 role이 바뀌는 경우의 더 적극적인 최적화는 state migration 정책으로 확장한다.

State migration의 wire artifact는 [state_migration_snapshot_format.md](state_migration_snapshot_format.md)에 정의한다. Snapshot은 모델 전체가 아니라 `model_fingerprint`와 worker-local CQDAG block/frontier/cache 상태만 담는다.

Tracker-side migration commit은 다음 규칙을 따른다.

```text
1. source worker가 node lease epoch e를 가진다.
2. tracker가 migration ticket을 prepare하고 source outbox에 MIGRATE_PREPARE를 넣는다.
3. source worker가 snapshot digest/payload를 보낸다.
4. tracker가 target outbox에 MIGRATE_INSTALL을 넣는다.
5. target worker가 같은 model_fingerprint 위에서 snapshot을 install하고 MIGRATE_ACK를 보낸다.
6. tracker가 commit하면 LeaseTable.transfer가 target epoch e+1을 발급한다.
7. 이후 source epoch e publish는 stale로 거부된다.
```

Commit 전 실패하면 migration은 abort되고 source lease가 유지된다. Commit 후 실패하면 target epoch만 유효하므로 같은 node range에 대해 두 worker가 동시에 publish할 수 없다.

Snapshot은 항상 옮기는 것이 아니라 `SnapshotPolicy`로 제한한다. Snapshot payload가 너무 크거나, snapshot 전송 비용이 해당 node를 다시 warm-up하는 비용보다 큰 경우에는 migration 대신 affinity 기반 재배치를 유지한다.

이 흐름은 실험용 callback이 아니라 distributed tracker의 기본 control path에 포함된다. Tracker는 worker별 control outbox를 가지며, worker가 `ready/chunk/exhausted` message를 보낸 뒤 다음 work를 받기 전에 pending migration control message를 먼저 받는다.

## Worker Join And Tracker Recovery

Worker는 실행 중 언제든 `ready` message로 합류할 수 있다. Tracker가 `expected_workers=None`으로 실행되는 경우, 이미 실행 중인 worker 수와 무관하게 새 worker를 `seen_workers`에 등록하고 다음 scheduler cycle에서 work를 배정한다. 따라서 worker 추가는 별도의 join phase가 아니라 normal control-plane message로 처리된다.

Tracker crash recovery는 worker lease를 복구하지 않는다. Lease는 tracker-local volatile state이므로, restart 후에는 모두 만료된 것으로 보고 새 epoch로 다시 배정한다. 대신 durable checkpoint는 다음 논리 상태를 저장한다.

```text
emitted_count
shard_cursors
emitted_stable_records
```

Restart한 tracker는 shard cursor를 checkpoint 위치로 옮기고, `ChunkStore` base offset도 같은 위치로 전진시킨다. 이전 tracker가 들고 있던 local chunk payload는 복구하지 않으며, 필요한 suffix chunk는 worker가 다시 materialize한다. 이렇게 하면 tracker crash 후에도 이미 발행한 top-k prefix를 다시 발행하지 않고, digest는 checkpoint에 저장된 stable record log로 이어서 계산한다.

## Consumer Retry

`CandidateBatch`는 `batch_id`, `start_rank`, `end_rank`로 식별된다. Consumer는 batch 처리 후 `BatchAck(DONE|FAILED)`를 ack plane으로 보낸다. Tracker는 `BatchRetryLedger`에 rank range와 attempt 상태를 남기고, 실패한 batch는 bounded inflight buffer에 있는 payload를 다시 발행한다.

```text
PUBLISHED -> INFLIGHT -> DONE
                    \-> FAILED -> INFLIGHT
```

이 규칙은 Hashcat, John the Ripper, 순수 hash verifier처럼 어떤 consumer를 붙이더라도 동일하다. Consumer 완료 순서는 top-k 의미가 아니며, 정확한 확률순 해석은 batch rank range와 앞선 batch 완료 여부로 판단한다.

## Model Fetch

Worker는 tracker와 같은 CQDAGPCFG 모델 fingerprint를 사용해야 한다. 기본 검증은 `model_fingerprint`로 수행한다. 기본 분산 경로에서는 worker가 전체 `model.json`을 받지 않고, tracker가 제공하는 `PagedModelManifest`와 `ModelJsonPage`를 통해 필요한 구조/슬롯 page만 fetch한다.

모델 page는 후보 생성이 필요한 node에만 올라간다. `NodeAgent`는 `LazyLocalResultSource`를 사용하므로 consumer나 idle role에서는 CQDAGPCFG 모델을 로드하지 않는다. generator role로 전환되어 실제 `read_range`가 호출될 때만 bounded JSON page cache를 준비하고 CQDAGPCFG source를 만든다.

Resource-only worker는 model path, targets path, control/batch/ack endpoint를 사전에 알 필요가 없다. Worker가 role plane에 붙으면 tracker-side `RoleController`가 `JobContext`를 내려준다.

```text
NodeAgent start
-> RoleClient bootstrap
-> RoleController replies JobContext
   - model_id / model_fingerprint
   - model_connect
   - control_connect / batch_connect / ack_connect
   - hash targets
   - source_mode / demand_window
-> consumer role: hash targets + CandidateBatch만 사용
-> generator role: needed model page만 lazy fetch
```

reference 구현은 다음 계층으로 나뉜다.

- `FilePagedModelArtifactStore`: tracker 쪽 page-backed model store. tracker가 canonical `model.json`을 읽고 구조 page와 slot entry page로 나누어 serve한다.
- `ZmqModelArtifactServer/ZmqModelArtifactClient`: `cqpcfg://` 위에서 manifest, raw artifact chunk, JSON page를 주고받는 ZeroMQ fetch plane.
- `PagedCQDAGRecordSource/PagedCQDAGStructureRecordSource`: worker 쪽 CQDAGPCFG source. 구조 page를 읽어 root/structure stream을 만들고, slot table entry는 실제 접근된 rank가 속한 page만 가져온다.
- `FileModelArtifactCache`: compatibility fallback. `--disable-paged-source` 또는 local full-model 실행이 필요할 때만 전체 artifact를 materialize한다.
- `BoundedModelPageCache`: raw artifact chunk/page를 bounded LRU로 유지하기 위한 낮은 수준의 cache primitive.
- `JobContext`: worker가 사전 설정 없이 참여할 수 있도록 role plane에서 내려주는 실행 context.

따라서 consumer-only node는 모델 메모리 없이 실행할 수 있고, generator로 바뀐 node도 전체 모델 대신 제한된 수의 JSON page만 유지한다. tracker는 모델 전체를 소유하고, worker는 CPU/GPU 자원과 page cache만 제공하는 구조다. 이 설계는 CQDAGPCFG 모델 로딩 메모리를 모든 worker에 중복 부과하지 않는 protocol-level model distribution layer다. 실제 배포에서는 같은 manifest/page 계약을 HTTP, object storage, shared volume 위에 올릴 수 있다.

주의할 점은 구조 목록은 전역 root/structure priority 계산에 필요하므로 generator source 생성 시 구조 page를 먼저 읽는다는 것이다. 반면 큰 메모리를 차지하는 slot table entry는 rank 접근 시점에 page 단위로 fetch되고, cache 한도를 넘으면 LRU로 회수된다.

실험에서 볼 핵심 메모리 지표는 다음 두 계층이다.

```text
tracker side:
  chunkstore_resident_records
  chunkstore_peak_resident_records
  chunkstore_reclaimed_records
  scheduler_affinity_hits
  scheduler_affinity_misses
  ack_pending_batches
  ack_republished_batches

generator side:
  source_cached_records
  source_peak_cached_records
  source_reclaimed_records
  source_dag_repository_active_units
  source_dag_stream_active_units
```
