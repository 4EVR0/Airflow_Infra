#!/usr/bin/env bash
# Airflow EC2 배포 — GitHub Actions(SSM)가 실행한다. 홈서버는 대상 아님(수동 반영).
#   scripts/deploy.sh auto <pull 이전 SHA>
#     git pull 직후 실행. DAG 실행에 영향 없는 서비스만 바로 반영한다.
#       alloy-airflow.alloy → alloy 재생성 / mapping.yml → statsd-exporter 재시작
#       airflow_exporter/** → airflow-observer 재빌드
#     docker-compose.yml이 바뀌면 scheduler·webserver가 다시 생성될 수 있어 '재시작 대기'만 표시한다.
#   scripts/deploy.sh restart
#     GitHub environment 승인 뒤 실행. compose up -d로 바뀐 서비스를 다시 만들고 대기 표시를 지운다.
# LocalExecutor라 scheduler 재생성 = 실행 중 태스크 종료 → 재시작은 사람이 빈 시간을 골라 승인한다.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"
PENDING=/var/tmp/airflow-restart-pending   # 승인 대기 중 다음 머지가 와도 재시작 필요를 잃지 않게
MODE="${1:-}"
git_() { git -c safe.directory="$REPO_DIR" "$@"; }

running() {   # 해당 서비스 컨테이너가 떠 있는지
  [[ -n "$(docker compose --profile observer ps -q --status running "$1" 2>/dev/null)" ]]
}

case "$MODE" in
  auto)
    BEFORE="${2:-}"; AFTER="$(git_ rev-parse HEAD)"
    echo "[deploy] $REPO_DIR  ${BEFORE:-(없음)} -> $AFTER"
    CHANGED=""
    if [[ -n "$BEFORE" && "$BEFORE" != "$AFTER" ]]; then CHANGED="$(git_ diff --name-only "$BEFORE" "$AFTER")"; fi
    echo "[deploy] 바뀐 파일:"; echo "${CHANGED:-(없음)}" | sed 's/^/  /'
    changed() { grep -qE "$1" <<<"$CHANGED"; }

    if changed '^alloy-airflow\.alloy$'; then
      echo "[deploy] Alloy 설정 변경 → alloy 재생성"
      docker compose up -d --no-deps --force-recreate alloy
    fi
    if changed '^mapping\.yml$'; then
      echo "[deploy] statsd 매핑 변경 → statsd-exporter 재시작"
      docker compose restart statsd-exporter
    fi
    if changed '^airflow_exporter/'; then
      if running airflow-observer; then
        echo "[deploy] observer 코드 변경 → airflow-observer 재빌드"
        docker compose --profile observer up -d --no-deps --build airflow-observer
      else
        echo "[deploy] observer 코드 변경 — 컨테이너가 꺼져 있어 건너뜀(opt-in profile)"
      fi
    fi
    if changed '^docker-compose\.yml$'; then
      echo "[deploy] docker-compose.yml 변경 → scheduler·webserver 재생성 가능성, 승인 후 재시작 대기"
      touch "$PENDING"
    fi
    # Actions가 이 줄을 읽어 승인 job 실행 여부를 정한다
    if [[ -f "$PENDING" ]]; then echo "RESTART_NEEDED=true"; else echo "RESTART_NEEDED=false"; fi
    ;;

  restart)
    echo "[restart] 실행 중 태스크 확인은 승인 전에 사람이 했다는 전제(Airflow 대시보드 observer 패널)"
    docker compose up -d
    if running airflow-observer; then docker compose --profile observer up -d --no-deps airflow-observer; fi
    for _ in $(seq 1 36); do
      if curl -fsS -o /dev/null --max-time 10 http://localhost:8080/health; then
        echo "[health] webserver OK"; rm -f "$PENDING"
        docker compose ps --format 'table {{.Service}}\t{{.Status}}'
        exit 0
      fi
      sleep 5
    done
    echo "[health] webserver 응답 없음" >&2; docker compose ps; exit 1
    ;;

  *)
    echo "사용: $0 auto <이전 SHA> | restart" >&2; exit 2 ;;
esac
