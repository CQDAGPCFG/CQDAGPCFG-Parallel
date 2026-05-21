# Docker 실행

이 구성은 CQDAGPCFG E2E 실험을 컨테이너 단위로 분리해서 실행한다.

```bash
docker compose -f experiments/docker/compose.yml up --build
docker compose -f experiments/docker/compose.yml down -v
```

구성은 다음 역할로 나뉜다.

- `prepare`: 학습 모델과 target hash 파일 생성
- `tracker`: CQDAGPCFG protocol tracker와 CandidateBatch 발행, data plane bind
- `generator-0..2`: ZeroMQ control plane에 붙는 generator worker
- `hash-consumer-0..1`: tracker data plane에 connect해서 CandidateBatch를 나눠 받고 순수 hash 비교 수행
- `verify-hits`: 여러 consumer hit report를 합쳐 target hash 발견 여부 확인

Docker는 프로세스와 네트워크를 분리해서 보는 용도이다. 로컬에서 빠르게 확인할 때는 다음 명령을 쓴다.

```bash
python experiments/src/run_local.py
```

이미 학습된 모델을 쓰려면 다음처럼 넘긴다.

```bash
python experiments/src/run_local.py \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json
```

실행 중 generator/consumer 비율 조정을 확인하려면 로컬 runner에서 dynamic mode를 켠다.

```bash
python experiments/src/run_local.py \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json \
  --limit 1000 \
  --dynamic-rebalance \
  --hash-delay-seconds 0.005
```

tracker crash 이후 durable recovery와 실행 중 worker 추가를 같이 보려면 fault-injection runner를 쓴다.

```bash
python experiments/src/fault_injection.py \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json \
  --limit 400
```

Docker에서 수동으로 같은 흐름을 볼 때는 checkpoint 파일을 남긴 상태로 tracker를 kill한 뒤 tracker를 다시 올리고 `fault` profile의 late worker를 추가한다.

```bash
docker compose -f experiments/docker/compose.yml up -d --build prepare hash-consumer-0 hash-consumer-1 tracker generator-0
docker compose -f experiments/docker/compose.yml kill -s SIGKILL tracker
docker compose -f experiments/docker/compose.yml up -d tracker
docker compose -f experiments/docker/compose.yml --profile fault up -d generator-late
```
