# CQDAGPCFG-Parallel Architecture

이 패키지는 `CQDAGPCFG`를 수정하지 않고 외부에서 병렬 프로토콜을 쌓기 위한 실험 공간이다.

## Boundary

- `adapters/cqdagpcfg`: `CQDAGPCFG`의 serial oracle과 optimized block API를 감싸는 얇은 경계
- `protocol`: node state, demand, lease, chunk manifest 같은 순수 protocol 자료형
- `simulation`: deterministic single-process simulator
- `runtime`: local worker/thread executor
- `control_plane`: 외부 worker 등록, heartbeat, task steal, chunk publish API
- `storage`: checkpoint, object store, manifest

## Rule

병렬 scheduling이나 worker 수가 바뀌어도 출력 의미론은 `CQDAGPCFG.OptimizedCQDAGEnumerator`의 prefix와 같아야 한다.
