#!/usr/bin/env bash
# Failure-injection drill: kill each Redis node in turn while load is running and
# report availability (non-5xx share) and which backend served the traffic.
#   docker compose up -d --build && ./scripts/chaos.sh
set -euo pipefail
URL=${URL:-http://localhost:8080}

probe() { # $1 = label, $2 = seconds
  local ok=0 total=0 end=$((SECONDS + $2)) code
  declare -A backends=()
  while (( SECONDS < end )); do
    out=$(curl -s -o /dev/null -D - "$URL/v1/check?key=user-$((RANDOM % 500))" || true)
    code=$(echo "$out" | awk 'NR==1{print $2}')
    b=$(echo "$out" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-ratelimit-backend"{print $2}')
    total=$((total + 1)); [[ "$code" == "200" || "$code" == "429" ]] && ok=$((ok + 1))
    backends[${b:-none}]=$(( ${backends[${b:-none}]:-0} + 1 ))
    sleep 0.01
  done
  printf '%-34s availability=%s/%s  backends:' "$1" "$ok" "$total"
  for k in "${!backends[@]}"; do printf ' %s=%s' "$k" "${backends[$k]}"; done; echo
}

probe "1. healthy" 5
docker compose stop redis-a >/dev/null;  probe "2. redis-a (primary) down" 8
docker compose stop redis-b >/dev/null;  probe "3. both Redis nodes down" 8
docker compose start redis-a redis-b >/dev/null; sleep 3
probe "4. recovered" 5
