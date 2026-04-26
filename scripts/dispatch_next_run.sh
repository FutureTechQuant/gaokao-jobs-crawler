#!/usr/bin/env bash
set -euo pipefail

WORKFLOW_FILE="${1:?workflow file is required}"
TARGET_REF="${2:?target ref is required}"
MODE="${3:?mode is required}"
YEARS="${4:-}"
CURRENT_YEAR="${5:?current year is required}"
CURRENT_SHARD="${6:?current shard is required}"

if [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "缺少 GITHUB_TOKEN"
  exit 1
fi

PAYLOAD="$(jq -nc   --arg ref "$TARGET_REF"   --arg mode "$MODE"   --arg years "$YEARS"   --arg current_year "$CURRENT_YEAR"   --arg current_shard "$CURRENT_SHARD"   '{
    ref: $ref,
    inputs: {
      mode: $mode,
      years: $years,
      current_year: $current_year,
      current_shard: $current_shard
    }
  }'
)"

HTTP_CODE="$(curl -sS   -o /tmp/dispatch_response.txt   -w '%{http_code}'   -X POST   -H 'Accept: application/vnd.github+json'   -H "Authorization: Bearer ${GITHUB_TOKEN}"   -H 'X-GitHub-Api-Version: 2022-11-28'   "https://api.github.com/repos/${GITHUB_REPOSITORY}/actions/workflows/${WORKFLOW_FILE}/dispatches"   -d "$PAYLOAD")"

if [ "$HTTP_CODE" != "204" ]; then
  echo "调度失败，HTTP ${HTTP_CODE}"
  cat /tmp/dispatch_response.txt
  exit 1
fi

echo "已调度下一次运行: year=${CURRENT_YEAR}, shard=${CURRENT_SHARD}"
