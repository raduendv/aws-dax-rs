#!/usr/bin/env bash
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
consumer=""
if [[ $# == 2 && "$1" == --consumer ]]; then
  consumer="$2"
  case "$consumer" in
    go) language_marker=Go ;;
    dotnet) language_marker=.NET ;;
    java) language_marker=Java ;;
    kotlin) language_marker=Kotlin ;;
    python) language_marker=Python ;;
    javascript) language_marker=JavaScript ;;
    *) echo "unknown consumer: $consumer" >&2; exit 2 ;;
  esac
elif [[ $# != 0 ]]; then
  echo "usage: $0 [--consumer go|dotnet|java|kotlin|python|javascript]" >&2
  exit 2
fi

if [[ "${DAX_FAULT_INJECTION:-}" != "1" ]]; then
  echo "Refusing to change firewall rules. Set DAX_FAULT_INJECTION=1 explicitly." >&2
  exit 2
fi

endpoint="${DAX_ENDPOINT:?DAX_ENDPOINT is required}"
table="${DAX_TABLE_NAME:-DaxParityTable}"
block_count="${DAX_BLOCK_COUNT:-1}"
if [[ ! "$block_count" =~ ^[1-2]$ ]]; then
  echo "DAX_BLOCK_COUNT must be 1 or 2" >&2
  exit 2
fi
ready_timeout="${DAX_FAULT_READY_TIMEOUT_SECONDS:-120}"
workload_timeout="${DAX_FAULT_WORKLOAD_TIMEOUT_SECONDS:-1800}"
for timeout in "$ready_timeout" "$workload_timeout"; do
  if [[ ! "$timeout" =~ ^[1-9][0-9]{0,3}$ ]]; then
    echo "fault timeouts must be whole seconds in 1..9999" >&2
    exit 2
  fi
done

declare -a blocked_ips=()
declare -a inserted_ips=()
barrier_dir=""
child_pid=""
rule_tag="aws-dax-fault-$$"
stop_child() {
  [[ -n "$child_pid" ]] || return 0
  # setsid keeps cargo/test descendants in the same explicitly owned process group.
  kill -TERM -- "-$child_pid" 2>/dev/null || true
  local deadline=$((SECONDS + 5))
  while kill -0 -- "-$child_pid" 2>/dev/null && ((SECONDS < deadline)); do sleep 0.1; done
  kill -KILL -- "-$child_pid" 2>/dev/null || true
  wait "$child_pid" 2>/dev/null || true
  child_pid=""
}
cleanup() {
  local status=$?
  local ip
  trap - EXIT
  trap '' INT TERM
  stop_child
  local failed=0
  if ((${#inserted_ips[@]} > 0)); then
    for ip in "${inserted_ips[@]}"; do
      if ! sudo -n iptables -D OUTPUT -d "$ip" -m comment --comment "$rule_tag" -j DROP; then
        echo "DAX_FAULT_CLEANUP delete=failed ip=${ip}" >&2
        failed=1
      fi
    done
    for ip in "${inserted_ips[@]}"; do
      if sudo -n iptables -C OUTPUT -d "$ip" -m comment --comment "$rule_tag" -j DROP 2>/dev/null; then
        echo "DAX_FAULT_CLEANUP status=failed ip=${ip}" >&2
        failed=1
      fi
    done
  fi
  if [[ -n "$barrier_dir" ]]; then
    rm -f "$barrier_dir/ready" "$barrier_dir/release" "$barrier_dir/log" || failed=1
    rmdir "$barrier_dir" || failed=1
  fi
  if ((failed)); then
    exit 1
  fi
  echo "DAX_FAULT_CLEANUP status=ok count=${#inserted_ips[@]}"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "$root"
if [[ -n "$consumer" || "${DAX_FAULT_PREWARM:-}" == "1" ]]; then
  command -v setsid >/dev/null || { echo "prewarm requires setsid (Linux util-linux)" >&2; exit 2; }
fi
if [[ -n "$consumer" ]]; then
  bash "$root/scripts/live_ffi_consumer.sh" build "$consumer"
fi

routes="$(
  AWS_REGION="${AWS_REGION:-eu-west-1}" \
    DAX_LIVE_TESTS=1 \
    DAX_ENDPOINTS="$endpoint" \
    DAX_ENDPOINT="$endpoint" \
    DAX_PRINT_ROUTES=1 \
    cargo test -p aws-dax --test live_dax exercises_live_endpoint_discovery_and_route_health \
      -- --ignored --nocapture 2>&1
)"
printf '%s\n' "$routes"

while read -r _ node address _; do
  ip="${address#address=}"
  ip="${ip%:*}"
  [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
  blocked_ips+=("$ip")
  ((${#blocked_ips[@]} >= block_count)) && break
done < <(printf '%s\n' "$routes" | awk '/DAX_ROUTE node=/ {
  ip=$3; sub(/:[^:]*$/, "", ip)
  if (!seen[ip]++) print $1, $2, $3, $4
}')

if ((${#blocked_ips[@]} != block_count)); then
  echo "Could not discover $block_count IPv4 route(s) to block." >&2
  exit 1
fi

insert_rules() {
  local ip
  for ip in "${blocked_ips[@]}"; do
    echo "Blocking outbound traffic to $ip"
    inserted_ips+=("$ip")
    sudo -n iptables -I OUTPUT -d "$ip" -m comment --comment "$rule_tag" -j DROP
  done
}
verify_rules() {
  local ip
  for ip in "${blocked_ips[@]}"; do
    sudo -n iptables -C OUTPUT -d "$ip" -m comment --comment "$rule_tag" -j DROP
  done
  echo "DAX_FAULT_RULES active=1 count=${#blocked_ips[@]}"
}

echo "Running live workload with ${#blocked_ips[@]} blocked route(s)"
workload_failures=0
run_prewarmed_workload() {
  barrier_dir="$(mktemp -d "${TMPDIR:-/tmp}/dax-fault.XXXXXX")"
  local ready_file="$barrier_dir/ready"
  local release_file="$barrier_dir/release"
  local log_file="$barrier_dir/log"
  local status
  setsid env AWS_REGION="${AWS_REGION:-eu-west-1}" \
      DAX_LIVE_TESTS=1 \
      DAX_ENDPOINTS="$endpoint" \
      DAX_ENDPOINT="$endpoint" \
      DAX_TABLE_NAME="$table" \
      DAX_FAULT_WORKLOAD=transaction \
      DAX_FAULT_PREWARM=1 \
      DAX_FAULT_READY_FILE="$ready_file" \
      DAX_FAULT_RELEASE_FILE="$release_file" \
      "$@" >"$log_file" 2>&1 &
  child_pid=$!
  local deadline=$((SECONDS + ready_timeout))
  while ((SECONDS < deadline)); do
    [[ -f "$ready_file" ]] && break
    kill -0 "$child_pid" 2>/dev/null || break
    sleep 0.1
  done
  if [[ ! -f "$ready_file" ]]; then
    echo "consumer exited before ready or timed out waiting for ready barrier" >&2
    stop_child
    cat "$log_file"
    exit 1
  fi
  insert_rules
  verify_rules
  : > "$release_file"
  deadline=$((SECONDS + workload_timeout))
  while kill -0 "$child_pid" 2>/dev/null; do
    if ((SECONDS >= deadline)); then
      echo "timed out waiting for fault workload" >&2
      stop_child
      cat "$log_file"
      exit 1
    fi
    sleep 0.1
  done
  if wait "$child_pid"; then status=0; else status=$?; fi
  child_pid=""
  cat "$log_file"
  echo "DAX_FAULT_STATUS workload=${consumer:-transaction} exit=${status}"
  if [[ -n "$consumer" ]]; then
    if [[ "$status" -ne 0 ]] || ! grep -Fxq \
      "$language_marker FFI direct, batch, transaction, paginator, route, and lifecycle operations passed" "$log_file"; then
      workload_failures=$((workload_failures + 1))
    fi
  elif [[ "$status" -ne 0 ]] || ! grep -Eq \
    'DAX_FAULT_RESULT workload=transaction operation=(TransactWriteItems|TransactGetItems) success=(0|1)' "$log_file"; then
    workload_failures=$((workload_failures + 1))
  fi
}
run_workload() {
  local workload="$1"
  local status
  local output
  echo "Starting ${workload} fault workload"
  set +e
  output="$(
    AWS_REGION="${AWS_REGION:-eu-west-1}" \
    DAX_LIVE_TESTS=1 \
    DAX_ENDPOINTS="$endpoint" \
    DAX_ENDPOINT="$endpoint" \
    DAX_TABLE_NAME="$table" \
    DAX_FAULT_WORKLOAD="$workload" \
    cargo test -p aws-dax --test live_dax exercises_live_plaintext_and_tls_smoke_operations \
      -- --ignored --nocapture 2>&1
  )"
  status=$?
  set -e
  printf '%s\n' "$output"
  echo "DAX_FAULT_STATUS workload=${workload} exit=${status}"
  if [[ "$workload" == transaction ]]; then
    if [[ "$status" -ne 0 || ! "$output" =~ DAX_FAULT_RESULT\ workload=transaction\ operation=(TransactWriteItems|TransactGetItems)\ success=(0|1) ]]; then
      workload_failures=$((workload_failures + 1))
    fi
  elif [[ "$status" -ne 0 || ! "$output" =~ DAX_FAULT_RESULT\ workload=${workload}\ operation=.*\ success=1 ]]; then
    workload_failures=$((workload_failures + 1))
  fi
}

if [[ -n "$consumer" ]]; then
  run_prewarmed_workload bash "$root/scripts/live_ffi_consumer.sh" run "$consumer"
elif [[ "${DAX_FAULT_PREWARM:-}" == "1" ]]; then
  run_prewarmed_workload cargo test -p aws-dax --test live_dax \
    exercises_live_plaintext_and_tls_smoke_operations -- --ignored --nocapture
else
  insert_rules
  verify_rules
  run_workload read
  run_workload write
  run_workload transaction
fi

verify_rules

if ((workload_failures > 0)); then
  echo "DAX_FAULT_SUMMARY status=failed workload_failures=${workload_failures}" >&2
  exit 1
fi
echo "DAX_FAULT_SUMMARY status=ok workload_failures=0"
