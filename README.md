# airflow-infra

4EVR0 Airflow 인프라 설정. EC2(메인)과 홈서버(크롤링 전용) 두 곳에서 운영.

## 구조

```
/home/airflow/
├── docker-compose.yml       # Airflow 2.9.2, LocalExecutor, PostgreSQL 15
├── .env                     # 실제 키 값 (gitignore — .env.example 참고)
├── .env.example
├── setup.sh                 # 새 서버 초기 세팅용
├── dags/                    # pipeline repo DAG 파일 symlink
└── pipelines/               # 각 pipeline repo clone 위치 (gitignore)
    ├── Oliveyoung_Crawling/
    ├── Oliveyoung_Pipeline/
    ├── INCI_Pipeline/
    └── GraphRAG_Pipeline/
```

## 새 서버 세팅 (홈서버 등)

### 1. OS 준비

```bash
# ECR credential helper 설치
sudo apt-get install -y amazon-ecr-credential-helper

# Docker config 디렉토리 생성
sudo mkdir -p /opt/airflow-docker
sudo tee /opt/airflow-docker/config.json > /dev/null <<'EOF'
{
  "credHelpers": {
    "973972497926.dkr.ecr.ap-northeast-2.amazonaws.com": "ecr-login"
  }
}
EOF
```

### 2. 이 repo clone

```bash
git clone git@github.com:4EVR0/airflow.git /home/airflow
cd /home/airflow
```

### 3. Pipeline repos 세팅

```bash
chmod +x setup.sh && ./setup.sh
```

### 4. DAG symlink 생성

EC2 기준으로 이미 커밋된 symlink를 확인하고 동일하게 생성:

```bash
cd dags/
ln -sf ../pipelines/<repo>/dags/<dag_file>.py <dag_name>.py
```

### 5. 환경변수 설정

```bash
cp .env.example .env
# .env 열어서 값 채우기
# EC2는 IAM Role 사용 → AWS 키 불필요
# 홈서버는 IAM 유저 키 직접 입력 필요
```

### 6. Airflow Connection 등록

```
Airflow UI → Admin → Connections → +
Connection Id : ec2_airflow
Connection Type: HTTP
Host: <EC2 Airflow 주소>
Port: 8080
```

### 7. 시작

```bash
docker compose up -d
```

## 운영

### Pipeline repo 업데이트

```bash
./setup.sh   # 전체 pull
# 또는 개별
git -C pipelines/Oliveyoung_Crawling pull
```

### Airflow 재시작

```bash
docker compose down && docker compose up -d
```
