#!/usr/bin/env bash
set -euo pipefail

WORKFLOW_FILE="${1:?workflow file is required}"
TARGET_REF="${2:?target ref is required}"
MODE="${3:?mode is required}"
YEARS="${4:-}"
CURRENT_YEAR="${5:?current year is required}"
CURRENT_PROVINCE_INDEX="${6:?current province index is required}"

if [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "缺少 GITHUB_TOKEN"
  exit 1
fi

API_ROOT="https://api.github.com/repos/${GITHUB_REPOSITORY}"

WORKFLOW_ID="$(
  curl -sS \
    -H 'Accept: application/vnd.github+json' \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H 'X-GitHub-Api-Version: 2022-11-28' \
    "${API_ROOT}/actions/workflows" \
  | jq -r --arg wf "$WORKFLOW_FILE" '
      .workflows[]
      | select(.path == $wf or (.path | endswith("/" + $wf)) or .name == $wf)
      | .id
    ' \
  | head -n 1
)"

if [ -z "${WORKFLOW_ID:-}" ] || [ "${WORKFLOW_ID}" = "null" ]; then
  echo "未找到目标 workflow: ${WORKFLOW_FILE}"
  echo "可见 workflow 列表如下："
  curl -sS \
    -H 'Accept: application/vnd.github+json' \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H 'X-GitHub-Api-Version: 2022-11-28' \
    "${API_ROOT}/actions/workflows" \
  | jq -r '.workflows[] | "\(.id)\t\(.name)\t\(.path)"'
  exit 1
fi

PAYLOAD="$(
  jq -nc \
    --arg ref "$TARGET_REF" \
    --arg mode "$MODE" \
    --arg years "$YEARS" \
    --arg current_year "$CURRENT_YEAR" \
    --arg current_province_index "$CURRENT_PROVINCE_INDEX" \
    '{
      ref: $ref,
      inputs: {
        mode: $mode,
        years: $years,
        current_year: $current_year,
        current_province_index: $current_province_index
      }
    }'
)"

HTTP_CODE="$(
  curl -sS \
    -o /tmp/dispatch_response.txt \
    -w '%{http_code}' \
    -X POST \
    -H 'Accept: application/vnd.github+json' \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H 'X-GitHub-Api-Version: 2022-11-28' \
    "${API_ROOT}/actions/workflows/${WORKFLOW_ID}/dispatches" \
    -d "$PAYLOAD"
)"

if [ "$HTTP_CODE" != "204" ] && [ "$HTTP_CODE" != "200" ]; then
  echo "调度失败，HTTP ${HTTP_CODE}"
  cat /tmp/dispatch_response.txt
  exit 1
fi

echo "已调度下一次运行: workflow_id=${WORKFLOW_ID}, year=${CURRENT_YEAR}, province_index=${CURRENT_PROVINCE_INDEX}"
