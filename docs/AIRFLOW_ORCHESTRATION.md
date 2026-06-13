# Airflow 파이프라인 오케스트레이션

## 전체 구조

```
/home/airflow/
├── docker-compose.yml          # Airflow 서버 (webserver + scheduler + postgres)
├── .env                        # AWS 자격증명, ECR_REGISTRY (직접 작성 필요)
└── dags/                       # 심볼릭 링크로 연결된 DAG 모음
    ├── inci_monthly_pipeline.py        → INCI_Pipeline/dags/
    ├── oliveyoung_pipeline.py          → Iceberg_pipeline/dags/
    └── oliveyoung_crawling_dag.py      → Oliveyoung_Crawling/dags/
```

---

## ECR 이미지 매핑

| 파이프라인 | ECR 리포지토리 | 빌드 트리거 |
|-----------|--------------|-----------|
| Oliveyoung 크롤러 | `evr0/oliveyoung-crawling` | main push |
| INCI 데이터 파이프라인 | `evr0/inci-pipeline` | main push |
| Iceberg ETL | `evr0/iceberg-pipeline` | main push |

---

## 파이프라인 실행 흐름

```
[3일마다 새벽 2시] ─── oliveyoung_crawling DAG
                         ecr_login
                             ↓
                         crawl (Playwright, 4GB 메모리, 2GB SHM)
                             ↓ S3에 raw JSON 저장
                         trigger_etl
                             ↓ TriggerDagRunOperator
                    oliveyoung_bronze_to_silver DAG (schedule=None)
                         ecr_login
                             ↓
                         sync_reference_data
                             ↓
                         bronze_to_silver
                             ↓
                         silver_to_gold

[매월 1일 01:00] ──── inci_monthly_pipeline DAG
                         ecr_login
                             ↓
                    bronze_kcia ──┐
                    bronze_cosing─┘
                             ↓ (둘 다 완료 후)
                         silver_mapping
                             ↓
                         gold_pipeline
```

---

## 초기 설정 가이드

### 1. `.env` 파일 생성

`/home/airflow/.env` 파일을 아래 형식으로 작성:

```
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>
ECR_REGISTRY=<account_id>.dkr.ecr.ap-northeast-2.amazonaws.com
OLIVEYOUNG_S3_BUCKET=<your-s3-bucket-name>
AIRFLOW_UID=50000
```

> `ECR_REGISTRY`의 `account_id`는 AWS Console → 우측 상단 계정 정보에서 확인.

### 2. Airflow 기동

```bash
cd /home/airflow
docker compose up -d
```

### 3. Airflow Variables 설정

Airflow 기동 후 WebUI(`http://localhost:8080`) 또는 CLI로 설정:

```bash
# WebUI: Admin → Variables
# 또는 CLI (airflow-scheduler 컨테이너 내부에서):
airflow variables set inci_project_dir "/home/airflow/pipelines/INCI_Pipeline"
```

| Variable 키 | 값 | 용도 |
|------------|---|------|
| `inci_project_dir` | `/home/airflow/pipelines/INCI_Pipeline` | INCI DAG의 .env 파일 로드 경로 |

### 4. INCI Pipeline .env 파일 확인

`/home/airflow/pipelines/INCI_Pipeline/.env`에 AWS 자격증명이 있어야 함:

```
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>
AWS_DEFAULT_REGION=ap-northeast-2
# INCI 파이프라인 필요 설정 추가
```

---

## DAG 설명

### `oliveyoung_crawling`
- **스케줄**: `0 2 */3 * *` (3일마다 새벽 2시)
- **역할**: Oliveyoung 웹사이트 Playwright 크롤링 → S3 raw JSON 저장
- **완료 후**: `oliveyoung_bronze_to_silver` DAG 자동 트리거
- **리소스**: 메모리 4GB, 공유메모리 2GB (Playwright 필수)

### `oliveyoung_bronze_to_silver`
- **스케줄**: 없음 (크롤링 DAG에서 트리거)
- **역할**: S3 raw JSON → Bronze → Silver → Gold Iceberg 테이블

### `inci_monthly_pipeline`
- **스케줄**: `0 1 1 * *` (매월 1일 01:00)
- **역할**: KCIA + CosIng API → Bronze → Silver 매핑 → Gold

---

## 주의사항

- ECR 토큰은 **12시간** 유효. 각 DAG에 `ecr_login` task가 있어 실행마다 갱신함.
- `oliveyoung_crawling` DAG의 크롤러는 `--s3-bucket` 인자로 `OLIVEYOUNG_S3_BUCKET` 환경변수를 사용.
- Airflow 컨테이너는 `/var/run/docker.sock`을 마운트해서 DockerOperator가 호스트 Docker 데몬 사용.
