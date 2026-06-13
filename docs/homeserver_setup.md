# 홈서버 Airflow 세팅 가이드

## 기술 선택 이유

### 왜 SimpleHttpOperator인가?

홈서버 크롤링 완료 후 EC2 Airflow를 트리거하는 방법은 여러 가지가 있다.

| 방법 | 설명 | 문제점 |
|--|--|--|
| cron + curl | 쉘 스크립트에서 curl로 EC2 호출 | 실패 시 재시도 없음, 로그가 파일에만 남음 |
| S3 센서 | S3에 마커 파일 올리면 EC2가 감지 | 최대 5분 지연, 추가 파일 관리 필요 |
| SQS | 메시지 큐로 완료 신호 전달 | SQS 큐 생성, IAM 권한 설정 등 인프라 추가 필요 |
| **SimpleHttpOperator** | Airflow 태스크로 EC2 REST API 직접 호출 | EC2 8080 포트 접근 필요 |

SimpleHttpOperator를 선택한 이유:

1. **Airflow 안에서 관리** — 크롤링과 트리거가 같은 DAG 안에 있어서 실패/성공 로그를 Airflow UI 한 곳에서 확인 가능
2. **재시도 자동화** — Airflow의 `retries` 설정으로 트리거 실패 시 자동 재시도. curl은 직접 구현해야 함
3. **커넥션 분리** — EC2 IP, 포트, 비밀번호를 코드에 하드코딩하지 않고 Airflow 커넥션으로 관리
4. **추가 인프라 없음** — SQS처럼 새로운 AWS 리소스를 만들 필요 없이 기존 Airflow REST API 활용

### 왜 홈서버에 Airflow를 띄우는가?

올리브영이 AWS IP 대역을 자동화로 감지하여 크롤링을 차단한다. 홈서버의 고정 IP에서 크롤링하면 일반 사용자 트래픽으로 인식된다. DockerOperator는 `unix://var/run/docker.sock`을 통해 홈서버 Docker 데몬을 사용하므로 컨테이너가 홈서버 IP로 트래픽을 발생시킨다.

홈서버에 Airflow를 두는 것이 단순 cron보다 나은 이유는 크롤링 실패 시 자동 재시도, 실행 히스토리 보관, 수동 재실행이 UI에서 가능하기 때문이다.

---

## 전체 구조

```
[홈서버 Airflow]
  oliveyoung_crawling DAG
    └─▶ DockerOperator (크롤러 실행, 홈서버 IP로 올리브영 크롤링)
    └─▶ SimpleHttpOperator (EC2 Airflow REST API 호출)

[EC2 Airflow]
  oliveyoung_bronze_to_silver DAG (트리거 수신 후 실행)
    └─▶ sync_reference → bronze_to_silver → silver_to_gold → silver_to_neo4j_csv
```

---

## 1. 사전 준비

### EC2 보안그룹 설정
홈서버 IP에서 EC2의 8080 포트 접근 허용 필요.

AWS 콘솔 → EC2 → 보안그룹 → 인바운드 규칙 추가:
- 유형: 사용자 지정 TCP
- 포트: 8080
- 소스: 홈서버 고정 IP/32

### AWS 자격증명
홈서버에서 ECR 이미지 pull을 위해 AWS 자격증명 필요:
```bash
aws configure
# AWS Access Key ID: ...
# AWS Secret Access Key: ...
# Default region: ap-northeast-2
```

---

## 2. 홈서버 Airflow 설치

### docker-compose.yml 작성
```yaml
version: '3.8'

x-airflow-common:
  &airflow-common
  image: apache/airflow:2.9.2
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    AWS_DEFAULT_REGION: ap-northeast-2
    ECR_REGISTRY: ${ECR_REGISTRY}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - /var/run/docker.sock:/var/run/docker.sock
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5

  airflow-init:
    <<: *airflow-common
    command: version
    entrypoint: /bin/bash -c "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8081:8080"   # EC2 Airflow가 8080이므로 홈서버는 8081 사용
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always
```

### .env 파일 작성
```
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>
ECR_REGISTRY=973972497926.dkr.ecr.ap-northeast-2.amazonaws.com
AIRFLOW_UID=50000
```

### 실행
```bash
docker compose up -d
# http://localhost:8081 접속 (admin/admin)
```

---

## 3. DAG 파일 작성

`dags/oliveyoung_crawling_dag.py` 생성:

```python
import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

ECR_REGISTRY = os.environ.get("ECR_REGISTRY", "")
S3_BUCKET = os.environ.get("OLIVEYOUNG_S3_BUCKET", "")

with DAG(
    dag_id="oliveyoung_crawling",
    schedule="0 2 */3 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["oliveyoung", "crawling"],
) as dag:

    crawl = DockerOperator(
        task_id="crawl",
        image=f"{ECR_REGISTRY}/evr0/oliveyoung-crawling:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        auto_remove="success",
        mount_tmp_dir=False,
        force_pull=True,
        mem_limit="4g",
        environment={
            "S3_BUCKET": S3_BUCKET,
            "RUN_ID": "{{ ds_nodash }}",
            "AWS_DEFAULT_REGION": "ap-northeast-2",
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        },
        execution_timeout=timedelta(hours=6),
    )

    trigger_ec2 = SimpleHttpOperator(
        task_id="trigger_ec2_pipeline",
        http_conn_id="ec2_airflow",
        endpoint="/api/v1/dags/oliveyoung_bronze_to_silver/dagRuns",
        method="POST",
        data=json.dumps({"conf": {}}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
    )

    crawl >> trigger_ec2
```

---

## 4. EC2 Airflow 커넥션 등록

홈서버 Airflow UI (http://localhost:8081) 접속 후:

```
Admin → Connections → + (추가)

Conn ID:   ec2_airflow
Conn Type: HTTP
Host:      http://{EC2_공인IP}
Port:      8080
Login:     admin
Password:  {EC2_Airflow_비밀번호}
```

---

## 5. ECR 이미지 pull 권한 확인

홈서버에서 아래 명령어로 ECR 접근 확인:
```bash
aws ecr get-login-password --region ap-northeast-2 \
  | docker login --username AWS --password-stdin \
    973972497926.dkr.ecr.ap-northeast-2.amazonaws.com
```

---

## 6. 동작 확인

1. 홈서버 Airflow UI에서 `oliveyoung_crawling` DAG 수동 트리거
2. `crawl` 태스크 로그에서 크롤링 진행 확인
3. 완료 후 `trigger_ec2_pipeline` 태스크 성공 확인
4. EC2 Airflow UI에서 `oliveyoung_bronze_to_silver` DAG 자동 실행 확인
