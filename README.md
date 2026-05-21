# CQDAGPCFG-Parallel

`CQDAGPCFG-Parallel`은 `CQDAGPCFG`를 라이브러리로 사용해서 병렬 실행 프로토콜을 실험하는 별도 패키지이다.

이 저장소의 원칙:

- `CQDAGPCFG` 본체에는 병렬 runtime을 섞지 않는다.
- `CQDAGPCFG`는 serial oracle과 block graph provider로만 사용한다.
- 병렬 protocol state, chunk, lease, scheduler, worker, merger는 이 패키지에서 관리한다.
- 초기 구현은 single-process simulator로 시작하고, correctness는 항상 `OptimizedCQDAGEnumerator` prefix와 비교한다.

## Protocol Contributions

논문에서 주장할 프로토콜 기여점은 코드상에서 다음 7개 축으로 분리한다.

1. 정확한 top-k 보존: serial CQDAGPCFG prefix와 distributed output digest를 비교한다.
2. Demand/Lease/ChunkStore 기반 local stream 공개: 필요한 prefix range만 생성하고 저장한다.
3. DAG-aware reclaim: 이미 소비된 local result와 CQDAG block 상태를 회수한다.
4. Tracker-owned model serving: tracker가 canonical model을 갖고 worker는 필요한 page/block만 fetch한다.
5. Page-backed model source: slot/structure JSON page를 디스크에 두고 worker cache 크기를 제한한다.
6. CQDAG-aware elastic role allocation: frontier, reclaim, page locality 신호로 generator/consumer 비율을 조정한다.
7. Durable batch/ack/retry boundary: candidate batch 발행 순서와 hash consumer 완료 순서를 분리한다.

## Layout

```text
src/cqdagpcfg_parallel/
  adapters/cqdagpcfg/  # CQDAGPCFG library boundary
  protocol/            # protocol state machine, chunk, lease, demand
  simulation/          # deterministic simulator and semantic checks
  runtime/             # local workers, threaded executor, metrics
  distributed/         # ZeroMQ tracker/worker distributed executor
  storage/             # checkpoint and durable chunk manifest types
tests/
  unit/                # state/chunk/lease 단위 테스트
  semantic/            # serial oracle prefix 비교 테스트
  fixtures/            # toy PCFG fixtures
docs/                  # 설계 문서
experiments/           # scheduling/performance experiments
```

## Development

먼저 sibling `CQDAGPCFG`를 editable로 설치한 뒤 이 패키지를 설치하는 흐름을 권장한다.

```bash
python -m pip install -e ../CQDAGPCFG
python -m pip install -e .[dev]
python -m pytest
```

## Experiment CLI

실험 실행은 `experiments/cqpcfg_experiment.py` 하나로 시작한다. `experiments/src` 아래의 `services`, `scenarios`, `tools`는 내부 구현 폴더이고 직접 실행할 필요가 없다.

```bash
python experiments/cqpcfg_experiment.py --help
python experiments/cqpcfg_experiment.py local --limit 1000
python experiments/cqpcfg_experiment.py validate
python experiments/cqpcfg_experiment.py tracker --model-path experiments/data/model.json --targets-path experiments/data/targets.json
CQPCFG_CONNECT=cqpcfg://tracker:5555 python experiments/cqpcfg_experiment.py worker
```

Docker에서도 같은 entrypoint를 사용한다. Tracker는 따로 띄우고, worker container는 필요할 때마다 추가로 실행해서 붙일 수 있다.

## Annotation API

```python
from cqdagpcfg_parallel.distributed import cqpcfg_distributed, cqpcfg_generator


@cqpcfg_distributed(limit=80, worker_count=3)
@cqpcfg_generator
def source(worker_id):
    return make_local_result_source(worker_id)


result, workers = source.run()
```

generator 전용 worker는 `@cqpcfg_worker`로 둘 수 있다. 분산 worker가 자기 자원 조건을 protocol에 알릴 때는 worker decorator에 선언한다.
Tracker는 이 값을 보고 역할 배정 입력으로 사용하지만, 자원량 자체를 강제로 정하지 않는다.

```python
from cqdagpcfg_parallel.distributed import cqpcfg_generator, cqpcfg_worker


@cqpcfg_worker(
    connect="cqpcfg://tracker:5555",
    worker_id="worker-0",
    resource_cpus=2.0,
    resource_memory="4g",
    resource_gpus=1,
    model_json_page_cache=64,
)
@cqpcfg_generator
def worker_source(worker_id):
    return make_local_result_source(worker_id)
```

생성과 소비를 모두 수행할 수 있는 elastic node는 `@cqpcfg_node_agent`로 묶는다.

```python
from cqdagpcfg_parallel.distributed import (
    cqpcfg_consumer,
    cqpcfg_generator,
    cqpcfg_node_agent,
)


@cqpcfg_node_agent(
    connect="cqpcfg://tracker:5555",
    node_id="node-0",
    resource_cpus=2.0,
    resource_memory="4g",
    resource_gpus=1,
    model_json_page_cache=64,
)
class WorkerNode:
    @cqpcfg_generator
    def source(self, worker_id):
        return make_local_result_source(worker_id)

    @cqpcfg_consumer
    def consume(self, batch):
        verify_candidates(batch.guesses)


WorkerNode.run()
```
