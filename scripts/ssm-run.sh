#!/usr/bin/env bash
# GitHub Actions 러너에서 실행 — SSM으로 원격 명령을 보내고 끝날 때까지 기다려 결과를 남긴다.
#   사용: scripts/ssm-run.sh <instance-id> <params.json> <comment>
#   원격 stdout은 ssm-stdout.txt에도 저장(다음 단계가 마커 줄을 읽음). Success가 아니면 exit 1.
set -euo pipefail
INSTANCE_ID="$1"; PARAMS="$2"; COMMENT="$3"

command_id=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --comment "$COMMENT" \
  --parameters "file://$PARAMS" \
  --query Command.CommandId --output text)
echo "SSM command: $command_id"

# 'aws ssm wait'은 약 100초 제한이라 직접 폴링(최대 15분)
for _ in $(seq 1 180); do
  status=$(aws ssm get-command-invocation --command-id "$command_id" --instance-id "$INSTANCE_ID" \
    --query Status --output text 2>/dev/null || echo Pending)
  case "$status" in Pending|InProgress|Delayed) sleep 5 ;; *) break ;; esac
done

aws ssm get-command-invocation --command-id "$command_id" --instance-id "$INSTANCE_ID" > ssm-result.json
jq -r .StandardOutputContent ssm-result.json > ssm-stdout.txt
echo "::group::stdout"; cat ssm-stdout.txt; echo "::endgroup::"
echo "::group::stderr"; jq -r .StandardErrorContent ssm-result.json; echo "::endgroup::"
echo "Status=$(jq -r .Status ssm-result.json) ResponseCode=$(jq -r .ResponseCode ssm-result.json)"
test "$(jq -r .Status ssm-result.json)" = "Success"
