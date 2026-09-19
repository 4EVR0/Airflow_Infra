# Airflow 상태 observer

Airflow 2.9.2의 **로컬 Stable REST API(v1)를 읽기 전용 GET으로 조회**하고,
Prometheus 텍스트 형식으로 `/metrics`에 노출하는 작은 exporter다.
각 Airflow 호스트에 하나씩 두며, 기존 Alloy가 내부 Docker 네트워크에서 스크레이프해
중앙 Prometheus로 remote_write한다. 홈서버(B)와 모니터링 서버(A)가 직접 통신할
필요는 없다. `/metrics`의 호스트 포트는 열지 않는다.

## 무엇을 측정하나

| 메트릭 | 뜻 |
|---|---|
| `airflow_observer_collection_success` | 가장 최근 전체 REST 조회 성공=1, 실패=0 |
| `airflow_observer_last_success_timestamp_seconds` | 마지막 정상 수집 시각(Unix 초) |
| `airflow_observer_dag_known{dag_id,paused}` | API에서 보이는 활성 DAG 목록 |
| `airflow_observer_dag_last_completed_state{dag_id,state}` | DAG별 가장 최근 완료 실행의 success/failed |
| `airflow_observer_dag_last_completed_end_timestamp_seconds{dag_id,state}` | 그 완료 시각 |
| `airflow_observer_dag_last_completed_duration_seconds{dag_id,state}` | 시작부터 종료까지의 초 |
| `airflow_observer_dag_active_run{dag_id,run_id,state}` | 현재 running/queued DAG 실행 |
| `airflow_observer_dag_active_run_start_timestamp_seconds{...}` | 시작한 현재 DAG 실행의 시작시각 |
| `airflow_observer_task_active{dag_id,run_id,task_id,map_index,state}` | 현재 running/queued 태스크 |
| `airflow_observer_task_start_timestamp_seconds{...}` | running 태스크 시작시각 |
| `airflow_observer_task_queued_timestamp_seconds{...}` | queued 태스크 대기 시작시각(있을 때) |
| `airflow_observer_active_run_count` / `airflow_observer_active_task_count` | 현재 실행·대기 건수 |

경과시간은 저장할 때 계속 증가하는 값이 아니라 Grafana/PromQL에서
`time() - airflow_observer_task_start_timestamp_seconds`처럼 계산한다. 따라서
초 단위가 명확하고, 현재 실행 건만 `run_id` 라벨을 둔다. **running은 작업이
진행 중이라는 증거가 아니다.** 크롤의 실제 진행/정체 판단은 별도 Loki 진행신호가 필요하다.

REST 조회는 DAG 목록, 각 DAG의 모든 run 페이지, 활성 run의 task 페이지 순서다.
API 응답은 `limit/offset/total_entries`로 끝까지 페이지네이션한다. 무한 조회를
막기 위해 DAG 200개, DAG당 run 1000개, 활성 run당 태스크 1000개를 상한으로 두고
초과 시 수집 실패로 표시한다. 기본 수집 간격 60초이고, Alloy는 30초마다 마지막
수집 결과를 스크레이프한다. 이력이 늘어 조회 비용이 커지면 조회 범위/캐시 설계를
재검토해야 한다.

API 호출 하나라도 실패하거나 응답 계약이 맞지 않으면
`airflow_observer_collection_success=0`이고, 이전 DAG·태스크 상태는 내보내지 않는다.
마지막 성공 수집이 3회 수집 간격 이상 오래돼도 같은 방식으로 관측 불가가 된다.
그래서 `active_task_count=0`(정상 수집 후 실행 건수 0)과 **관측 불가**를 구분한다.
Airflow 웹서버 자체가 죽은 경우에는 이 상세 수집도 실패하지만, 기존 StatsD
heartbeat와 node-exporter는 독립적으로 남는다.

## 테스트

```bash
cd Airflow_Infra
python -m unittest discover -s airflow_exporter/tests -v
```

## 크롤 중 안전한 첫 배포

1. **홈서버의 Airflow UI**에서 `Viewer` 역할의 exporter 전용 계정을 만든다.
   관리자 계정을 재사용하지 않는다. 비밀번호는 Git에 넣지 않고 홈서버
   `Airflow_Infra/.env`에 `AIRFLOW_EXPORTER_USERNAME/PASSWORD`로 설정한다.
2. 홈서버에 이 레포 변경을 반영한 뒤 아래 **대상 서비스만** 실행한다.
   실행 중인 Airflow scheduler/webserver/postgres는 재시작하지 않는다.

   ```bash
   docker compose --profile observer up -d --no-deps --build airflow-observer
   docker compose exec airflow-observer python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:9110/metrics').read().decode())"
   ```

3. 출력에서 `airflow_observer_collection_success 1`과 현재 크롤의
   `airflow_observer_task_active`·시작시각을 확인한다. 0이면
   `docker compose logs --tail=50 airflow-observer`로 401/403/API 오류를 본다.
   비밀번호나 Authorization 헤더가 로그에 나오지 않게 공유 로그를 확인한다.
4. 중앙 Prometheus에서도 보려면 **Alloy만** 새 설정으로 다시 띄운다.
   이 작업은 Airflow 실행 자체에는 영향이 없지만 로그/메트릭 전송이 잠깐 끊길
   수 있으므로 현재 크롤 중인지 감안해 시점을 정한다.

   ```bash
   docker compose up -d --no-deps --force-recreate alloy
   ```

5. Prometheus에서 `airflow_observer_collection_success{host="<홈서버 HOST_NAME>"}`를
   확인한다. EC2에도 같은 순서를 반복한다. EC2 배포 자동화는 현재 Git pull만 하므로
   새 서비스의 build/up은 별도로 필요하다.

observer는 `observer` Compose profile로 opt-in이다. 계정 준비 전 일반
`docker compose up -d`가 observer를 시작하지는 않지만, 현재 크롤 중에는 전체
스택 `up`이나 `restart`를 무심코 실행하지 말 것. 실행 중인 Airflow 컨테이너까지
재생성/재시작할 수 있다.
