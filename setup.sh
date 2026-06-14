#!/bin/bash
# 새 서버 초기 세팅 또는 pipeline repo 업데이트 시 실행
# 전제: amazon-ecr-credential-helper 설치 + /opt/airflow-docker/config.json 생성 완료
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== pipeline repos ==="
mkdir -p "$SCRIPT_DIR/pipelines"

sync_repo() {
local repo=$1
local dir="$SCRIPT_DIR/pipelines/$repo"
if [ -d "$dir/.git" ]; then
    echo "  pull: $repo"
    git -C "$dir" pull
else
    echo "  clone: $repo"
    git clone "git@github.com:4EVR0/$repo.git" "$dir"
fi
}

sync_repo Oliveyoung_Crawling
sync_repo Oliveyoung_Pipeline
sync_repo INCI_Pipeline
sync_repo GraphRAG_Pipeline

echo ""
echo "=== done ==="
echo "다음 단계:"
echo "  1. .env.example → .env 복사 후 값 채우기"
echo "  2. docker compose up -d"