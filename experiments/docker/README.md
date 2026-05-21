# Docker 실행

이 구성은 CQDAGPCFG E2E 실험을 컨테이너 단위로 분리해서 실행한다.

```bash
docker compose -f experiments/docker/compose.yml up --build
docker compose -f experiments/docker/compose.yml down -v
```

실행 후 대시보드는 다음 주소에서 볼 수 있다.

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3300
- Grafana 계정: `admin` / `admin`
- Dashboard: `CQDAGPCFG / CQDAGPCFG Protocol Overview`

구성은 다음 역할로 나뉜다.

- `prepare`: 학습 모델과 target hash 파일 생성
- `tracker`: CQDAGPCFG protocol tracker, role controller, model page server, CandidateBatch 발행
- `node-0..4`: `node_agent.py` 기반 elastic worker. 현재 role에 따라 generator 또는 consumer로 동작
- `metrics-exporter`: `/artifacts/metrics/*.json`을 Prometheus `/metrics` 형식으로 변환
- `prometheus`: protocol metrics scrape
- `grafana`: Prometheus datasource와 CQDAGPCFG overview dashboard 자동 provisioning
- `verify-hits`: 여러 node hit report를 합쳐 target hash 발견 여부 확인

## Tracker와 worker 분리 실행

tracker를 호스트나 외부 서버에서 따로 띄우고, Docker는 worker node만 여러 개 붙일 수도 있다. 이 모드는 worker가 모델 파일을 직접 들고 시작하지 않는다. worker는 role controller에서 job context를 받고, 필요한 모델 block/page만 tracker의 model artifact server에서 가져온다.

먼저 tracker가 사용할 model/target artifact를 만든다.

```bash
mkdir -p /tmp/cqdagpcfg-split
python experiments/cqpcfg_experiment.py prepare \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json \
  --model-path /tmp/cqdagpcfg-split/model.json \
  --targets-path /tmp/cqdagpcfg-split/targets.json \
  --limit 1000
```

그 다음 tracker를 호스트에서 실행한다. 같은 컴퓨터의 Docker worker가 붙는 경우 `--advertise-host host.docker.internal`을 사용한다. 외부 서버에서 tracker를 띄운다면 이 값을 서버 IP나 DNS 이름으로 바꾼다.

```bash
python experiments/cqpcfg_experiment.py tracker \
  --model-path /tmp/cqdagpcfg-split/model.json \
  --targets-path /tmp/cqdagpcfg-split/targets.json \
  --bind cqpcfg://0.0.0.0:5555 \
  --advertise-host host.docker.internal \
  --source-mode structure \
  --metrics-path /tmp/cqdagpcfg-split/metrics/tracker.json
```

worker는 필요한 시점에 하나씩 붙인다. tracker는 노드 수를 미리 알 필요가 없다. 같은 명령을 여러 번 실행하면 매번 새 worker 컨테이너가 하나씩 추가된다.

```bash
TRACKER_HOST=host.docker.internal \
./experiments/docker/run_worker.sh

TRACKER_HOST=host.docker.internal \
./experiments/docker/run_worker.sh

TRACKER_HOST=host.docker.internal \
./experiments/docker/run_worker.sh
```

`run_worker.sh`는 worker image가 없을 때만 build한다. 코드를 바꾼 뒤 강제로 다시 build하려면 `CQPCFG_FORCE_BUILD=1`을 붙인다.

worker별 CPU, 메모리, GPU도 컨테이너 단위로 제한할 수 있다.

```bash
TRACKER_HOST=host.docker.internal \
CQPCFG_WORKER_CPUS=2 \
CQPCFG_WORKER_MEMORY=4g \
CQPCFG_MODEL_JSON_PAGE_CACHE=64 \
./experiments/docker/run_worker.sh
```

같은 값은 worker capability로도 role protocol에 보고된다. CQDAGPCFG model page cache 같은 내부 메모리 사용량도 worker가 `CQPCFG_MODEL_JSON_PAGE_CACHE`로 직접 고른다. tracker는 worker가 보고한 capability를 보고 역할 배정만 수행한다.

role별 최소 자원도 프로토콜에서 지정할 수 있다. 예를 들어 GPU가 있는 node만 consumer 역할을 받게 하려면 다음처럼 둔다.

```bash
python experiments/cqpcfg_experiment.py tracker \
  --model-path /tmp/cqdagpcfg-split/model.json \
  --targets-path /tmp/cqdagpcfg-split/targets.json \
  --bind cqpcfg://0.0.0.0:5555 \
  --advertise-host host.docker.internal \
  --source-mode structure \
  --consumer-min-gpus 1
```

GPU를 노출해야 하는 consumer worker는 Docker의 `--gpus` 값을 그대로 넘긴다.

```bash
TRACKER_HOST=host.docker.internal \
CQPCFG_WORKER_GPUS=all \
CQPCFG_RESOURCE_GPUS=1 \
CQPCFG_WORKER_CPUS=4 \
CQPCFG_WORKER_MEMORY=8g \
./experiments/docker/run_worker.sh
```

특정 GPU만 붙일 수도 있다.

```bash
TRACKER_HOST=host.docker.internal \
CQPCFG_WORKER_GPUS='"device=0"' \
./experiments/docker/run_worker.sh
```

지원하는 resource 변수는 다음과 같다.

| 변수 | Docker 옵션 | 예시 |
|---|---|---|
| `CQPCFG_WORKER_CPUS` | `--cpus` | `2`, `0.5` |
| `CQPCFG_WORKER_CPUSET_CPUS` | `--cpuset-cpus` | `0-3`, `0,2` |
| `CQPCFG_WORKER_MEMORY` | `--memory` | `4g`, `512m` |
| `CQPCFG_WORKER_MEMORY_RESERVATION` | `--memory-reservation` | `2g` |
| `CQPCFG_WORKER_MEMORY_SWAP` | `--memory-swap` | `4g`, `-1` |
| `CQPCFG_WORKER_SHM_SIZE` | `--shm-size` | `1g` |
| `CQPCFG_WORKER_PIDS_LIMIT` | `--pids-limit` | `256` |
| `CQPCFG_WORKER_GPUS` | `--gpus` | `all`, `"device=0"` |
| `CQPCFG_RESOURCE_CPUS` | protocol-reported CPU capability | `2` |
| `CQPCFG_RESOURCE_MEMORY` | protocol-reported memory capability | `4g` |
| `CQPCFG_RESOURCE_GPUS` | protocol-reported GPU count | `1` |
| `CQPCFG_RESOURCE_GPU_MEMORY` | protocol-reported GPU memory | `8g` |
| `CQPCFG_MODEL_JSON_PAGE_CACHE` | worker-selected model page cache | `64` |

기본 역할 배정은 먼저 최소 consumer와 generator를 채우고, 이후 늦게 붙는 worker는 generator로 배정한다. 기본값은 `min-consumers=1`, `min-generators=1`, `late-worker-role=generator`이다.

`--bind cqpcfg://0.0.0.0:5555`는 public protocol endpoint 하나만 받는 설정이다. 내부 구현은 다음 subchannel을 자동으로 파생한다.

| 역할 | URI |
|---|---|
| control | `cqpcfg://<host>:5555` |
| batch | `cqpcfg://<host>:5556` |
| role | `cqpcfg://<host>:5557` |
| ack | `cqpcfg://<host>:5558` |
| model page/block | `cqpcfg://<host>:5559` |

worker를 한 번에 여러 개 붙이고 싶을 때만 compose scale을 쓴다.

```bash
TRACKER_HOST=host.docker.internal \
docker compose -f experiments/docker/workers.compose.yml up -d --build --scale node-agent=5
```

worker 대시보드까지 같이 띄우려면 compose에서 service 이름을 생략한다. 이미 `9090`, `9108`, `3300` 포트를 쓰고 있다면 `CQPCFG_PROMETHEUS_PORT`, `CQPCFG_METRICS_PORT`, `CQPCFG_GRAFANA_PORT` 환경 변수로 바꾼다.

외부 tracker에 붙일 때는 다음처럼 tracker 주소만 바꾼다.

```bash
TRACKER_HOST=tracker.example.com \
./experiments/docker/run_worker.sh
```

이때 Docker worker는 `node_agent.py`로 실행되며, 각 컨테이너는 고유 hostname을 node id로 사용한다. tracker의 role controller는 접속 순서대로 최소 consumer/generator를 채우고, 이후 늦게 붙는 worker에는 `late-worker-role`을 적용한다.

실행 중인 worker 컨테이너를 정리하려면 다음 명령을 쓴다.

```bash
./experiments/docker/stop_workers.sh
```

Docker는 프로세스와 네트워크를 분리해서 보는 용도이다. 로컬에서 빠르게 확인할 때는 다음 명령을 쓴다.

```bash
python experiments/cqpcfg_experiment.py local
```

이미 학습된 모델을 쓰려면 다음처럼 넘긴다.

```bash
python experiments/cqpcfg_experiment.py local \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json
```

실행 중 generator/consumer 비율 조정을 확인하려면 로컬 runner에서 dynamic mode를 켠다.

```bash
python experiments/cqpcfg_experiment.py local \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json \
  --limit 1000 \
  --dynamic-rebalance \
  --hash-delay-seconds 0.005
```

tracker crash 이후 durable recovery와 실행 중 worker 추가를 같이 보려면 fault-injection runner를 쓴다.

```bash
python experiments/cqpcfg_experiment.py fault \
  --source-model-path ../CQDAGPCFG/examples/artifacts/rockyou_train/model.json \
  --limit 400
```

Docker에서 수동으로 같은 흐름을 볼 때는 checkpoint 파일을 남긴 상태로 tracker를 kill한 뒤 tracker를 다시 올리고 `fault` profile의 late worker를 추가한다.

```bash
docker compose -f experiments/docker/compose.yml up -d --build prepare tracker node-0 node-1 node-2
docker compose -f experiments/docker/compose.yml kill -s SIGKILL tracker
docker compose -f experiments/docker/compose.yml up -d tracker
docker compose -f experiments/docker/compose.yml --profile fault up -d node-late
```
