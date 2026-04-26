#!/usr/bin/env bash
set -euo pipefail

RAW_YEARS="${1:-}"
MODE="${2:-test}"
CURRENT_YEAR="$(date +%Y)"

trim() {
  local s="$1"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf '%s' "$s"
}

emit_year() {
  local y
  y="$(trim "$1")"
  if [[ "$y" =~ ^[0-9]{4}$ ]]; then
    echo "$y"
  fi
}

if [ -z "$(trim "$RAW_YEARS")" ]; then
  case "$MODE" in
    test)
      emit_year "$CURRENT_YEAR"
      ;;
    sample|full)
      emit_year "$CURRENT_YEAR"
      emit_year "$((CURRENT_YEAR - 1))"
      emit_year "$((CURRENT_YEAR - 2))"
      ;;
    *)
      emit_year "$CURRENT_YEAR"
      ;;
  esac
  exit 0
fi

if [[ "$RAW_YEARS" == *-* ]]; then
  START_RAW="$(trim "${RAW_YEARS%%-*}")"
  END_RAW="$(trim "${RAW_YEARS#*-}")"

  if [[ ! "$START_RAW" =~ ^[0-9]{4}$ ]] || [[ ! "$END_RAW" =~ ^[0-9]{4}$ ]]; then
    echo "年份范围格式错误: $RAW_YEARS" >&2
    exit 1
  fi

  START_YEAR="$START_RAW"
  END_YEAR="$END_RAW"

  if [ "$START_YEAR" -ge "$END_YEAR" ]; then
    for ((y=START_YEAR; y>=END_YEAR; y--)); do
      echo "$y"
    done
  else
    for ((y=END_YEAR; y>=START_YEAR; y--)); do
      echo "$y"
    done
  fi
  exit 0
fi

IFS=',' read -r -a PARTS <<< "$RAW_YEARS"
FOUND=0
for part in "${PARTS[@]}"; do
  year="$(trim "$part")"
  if [[ "$year" =~ ^[0-9]{4}$ ]]; then
    echo "$year"
    FOUND=1
  fi
done

if [ "$FOUND" -eq 0 ]; then
  echo "没有可执行年份" >&2
  exit 1
fi
