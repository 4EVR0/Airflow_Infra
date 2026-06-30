# Airflow_Infra

- 4EVR0 데이터 파이프라인을 돌리는 Airflow 운영 환경
- DAG 정의는 각 파이프라인 레포에 두고, 이 레포는 그 DAG 를 묶어 실행하는 인프라(Compose 스택,
DAG 심링크, 메트릭/로그 수집)를 관리한다.
- EC2(메인)와 홈서버(크롤링 전용) 두 곳에서 같은 코드로 운영한다.

## 구성

```
docker-compose.yml      Airflow 2.9.2 · LocalExecutor · PostgreSQL 15
                        + statsd/node exporter · Alloy (메트릭·로그 수집)
alloy-airflow.alloy     로그 → Loki, 메트릭 → Prometheus remote_write (push)
mapping.yml             statsd → Prometheus 메트릭 매핑
setup.sh                파이프라인 레포 clone/pull
dags/                   각 파이프라인 레포의 DAG 파일 심링크
pipelines/              파이프라인 레포 clone 위치 (gitignore)
docs/                   오케스트레이션·홈서버 세팅 메모
```

`dags/` 의 파일은 `pipelines/<repo>/dags/<file>.py` 를 가리키는 심링크
DAG 코드는 해당 파이프라인 레포에서 관리하고, 여기서는 심링크만 커밋한다

## DAG

| DAG | 스케줄 | 흐름 |
|-----|--------|------|
| `oliveyoung_crawling` | 3일마다 | crawl → 파이프라인 트리거 |
| `oliveyoung_pipeline` | 트리거 | sync_reference → bronze_to_silver → silver_to_gold → neo4j_incremental |
| `oliveyoung_silver_to_neo4j_csv` | 수동 | Neo4j 초기 적재 (일회성) |
| `inci_monthly_pipeline` | 매월 1일 | bronze(kcia·cosing 병렬) → silver_mapping → gold |

파이프라인 태스크는 ECR 이미지를 받아 DockerOperator 로 실행한다.

## 모니터링

별도의 공용 모니터링 서버(Prometheus·Grafana·Loki)로 메트릭과 로그를 보낸다.
이 스택 안의 Alloy 가 수집·전송을 담당한다.

- **메트릭**: statsd-exporter(DAG/태스크 지표) + node-exporter(호스트)를 Alloy 가 스크레이프해
  Prometheus 로 `remote_write` push. `host` 라벨로 EC2/홈서버를 구분한다
- **로그**: Airflow 태스크 로그(`/opt/airflow/logs`)를 Alloy 가 tail 해 Loki 로 push
  `dag_id` / `task_id` 라벨을 붙여 Grafana 에서 메트릭 → 로그로 점프할 수 있다

홈서버는 모니터링 서버와 망이 달라 pull 이 안 되므로, 양쪽 모두 Alloy push 방식으로 통일했다

## 세팅

전제: `amazon-ecr-credential-helper` 설치 + ECR credHelper 용 docker config 준비.

```bash
git clone git@github.com:4EVR0/Airflow_Infra.git /home/airflow
cd /home/airflow

./setup.sh                 # 파이프라인 레포 clone/pull
cp .env.example .env        # 값 채우기 (EC2 는 IAM Role, 홈서버는 IAM 유저 키)

docker compose up -d
```

DAG 심링크는 EC2 기준으로 커밋돼 있다. 새 서버라면 동일하게 만든다.

```bash
ln -sf ../pipelines/<repo>/dags/<file>.py dags/<dag_name>.py
```

## 운영

```bash
./setup.sh                              # 파이프라인 레포 전체 업데이트
git -C pipelines/<repo> pull             # 개별 업데이트
docker compose restart                   # Airflow 재시작
```

> Alloy 설정(`.env`)을 바꾼 뒤에는 `docker compose up -d --force-recreate alloy`.
> 그냥 `up` 만 하면 env_file 변경이 반영되지 않는다.

## 메모

- 웹 UI: `:8080` / Postgres·logs·pipelines 는 볼륨/바인드 마운트로 영속화
- 파이프라인 태스크는 단발 컨테이너라 직접 스크레이프가 안 된다. 커스텀 메트릭은 Pushgateway 도입 예정
