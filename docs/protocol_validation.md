# CQDAGPCFG Protocol Validation

이 문서는 CQDAGPCFG-Parallel 프로토콜이 serial CQDAGPCFG의 후보 prefix를 보존하면서 분산 생성, hash consumer, reclaim, 장애 복구, role hot-swap을 처리하는지 검증하는 기준을 정리한다.

## 검증 범위

현재 검증은 다음 다섯 단계로 구성한다.

1. Large-limit prefix 검증
2. Tracker crash 이후 durable recovery와 late worker join
3. Network/IO 병목 측정
4. 핵심 정책 ablation
5. 결과 요약 문서화

반복 실행은 다음 스크립트로 수행한다.

```bash
python experiments/src/protocol_validation.py \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json \
  --limits 1000 20000 \
  --large-limit 100000
```

ZeroMQ 실행에는 `pyzmq`가 필요하다.

## 최신 검증 결과

실행 결과 디렉터리:

```text
experiments/results/protocol_validation/20260521-030337
```

요약:

| 항목 | 결과 |
|---|---:|
| 전체 결과 | PASS |
| 최대 prefix | 100000 |
| serial digest 일치 | 통과 |
| hash consumer 검증 | 통과 |
| tracker crash recovery | 통과 |
| late worker join | 통과 |
| dynamic role hot-swap | 관측됨 |
| default peak resident records at 100000 | 76 |
| default reclaimed records at 100000 | 100000 |

## 핵심 관찰

Large-limit run에서는 `1000`, `20000`, `100000`개 후보 prefix 모두 serial CQDAGPCFG digest와 일치했다. `max_parallel_leases_per_node=2`로 range lease를 켠 상태에서도 top-k 의미가 유지되었다.

Fault run에서는 tracker를 durable checkpoint 이후 강제로 종료한 뒤 restart했고, late worker를 새로 붙였다. 최종 digest는 serial과 같았고 hash consumer도 target을 찾았다.

Bottleneck run에서는 persistent `NodeAgent`가 in-process role switch를 수행했다. 측정된 값은 다음과 같다.

| 지표 | 값 |
|---|---:|
| emitted records | 5000 |
| peak resident records | 74 |
| reclaimed records | 5000 |
| data sent messages | 318 |
| data sent bytes | 428788 |
| data bytes/candidate | 85.758 |
| recv poll seconds | 1.520151 |
| ack messages | 626 |
| role control messages | 152 |
| role control seconds | 0.006734 |

현재 병목 신호는 serialization/send 자체보다 consumer polling/대기 시간 쪽이 더 크다. 즉 프로토콜의 control plane 비용은 작고, 실험 설정에서는 consumer side polling과 batch 소비 속도가 더 먼저 관측된다.

## Ablation 결과

`limit=5000` 기준 ablation 결과는 다음과 같다.

| 설정 | Digest | Peak resident | Reclaimed |
|---|---|---:|---:|
| default | 일치 | 76 | 5000 |
| single-range lease | 일치 | 76 | 5000 |
| no reclaim | 일치 | 5015 | 0 |
| no affinity | 일치 | 75 | 5000 |

가장 중요한 결과는 `no reclaim`이다. 정확성은 유지되지만 resident records가 prefix 길이에 비례해 증가한다. 반면 default는 `5000`개를 발행해도 peak resident가 `76`으로 유지된다. 따라서 reclaim은 단순 최적화가 아니라 memory-optimized protocol contribution의 핵심이다.

## 프로토콜 불변조건

현재 구현이 지켜야 하는 핵심 불변조건은 다음과 같다.

- `EnumerationChunk(node, [a,b))`는 serial CQDAGPCFG local stream의 같은 구간과 동일해야 한다.
- Lease는 node 전체가 아니라 `[start,end)` range를 소유한다.
- 서로 겹치는 active range lease는 허용하지 않는다.
- out-of-order chunk는 `ChunkStore` pending 영역에 보관하고, contiguous prefix가 될 때만 `ready_end`를 전진한다.
- 미래 range에서 EOF를 먼저 보더라도, prefix가 해당 지점까지 준비되기 전에는 shard를 exhausted로 확정하지 않는다.
- Merger가 발행하는 `CandidateBatch`의 논리적 연결은 serial CQDAGPCFG top-k prefix와 같아야 한다.
- 이미 발행된 prefix는 tracker checkpoint와 stable record log로 복구 가능해야 한다.

## 논문에 넣을 핵심 문장

이 프로토콜의 핵심 기여는 CQDAGPCFG를 단순히 worker에 올린 것이 아니라, CQDAG local stream을 range lease와 demand-driven chunk로 분산 materialize하면서 serial top-k 의미를 보존하고, DAG-aware reclaim으로 candidate prefix 생성의 메모리 사용량을 prefix 길이와 분리한 점이다.
