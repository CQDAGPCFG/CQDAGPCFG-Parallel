# CQDAGPCFG-Parallel

`CQDAGPCFG-Parallel`은 `CQDAGPCFG`를 라이브러리로 사용해서 병렬 실행 프로토콜을 실험하는 별도 패키지이다.

이 저장소의 원칙:

- `CQDAGPCFG` 본체에는 병렬 runtime을 섞지 않는다.
- `CQDAGPCFG`는 serial oracle과 block graph provider로만 사용한다.
- 병렬 protocol state, chunk, lease, scheduler, worker, merger는 이 패키지에서 관리한다.
- 초기 구현은 single-process simulator로 시작하고, correctness는 항상 `OptimizedCQDAGEnumerator` prefix와 비교한다.

## Layout

```text
src/cqdagpcfg_parallel/
  adapters/cqdagpcfg/  # CQDAGPCFG library boundary
  protocol/            # protocol state machine, chunk, lease, demand
  simulation/          # deterministic simulator and semantic checks
  runtime/             # local workers, threaded executor, metrics
  control_plane/       # future HTTP/WebSocket/gRPC control plane
  storage/             # future checkpoint and durable chunk manifest
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
