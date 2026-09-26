#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
project="rm-quorum-$(date +%s)-$$-${RANDOM}"
compose=(docker compose --project-name "$project" --file "$repo/tests/integration/quorum-compose.yaml")
context=$(docker context show)
endpoint=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}')
if [[ -n ${DOCKER_HOST:-} || "$endpoint" != unix://* ]]; then
  echo 'Quorum proof requires a local Unix-socket Docker context without DOCKER_HOST' >&2
  exit 1
fi
docker info >/dev/null
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/$project-build.XXXXXX")
cleanup() {
  result=$?
  trap - EXIT
  if ((result != 0)); then
    "${compose[@]}" logs --no-color --tail 50 || true
    "${compose[@]}" exec -T rabbitmq2 sh -c 'test ! -f /tmp/quorumproof-ack-take.log || cat /tmp/quorumproof-ack-take.log' || true
  fi
  if ! "${compose[@]}" down --volumes --remove-orphans --timeout 10; then result=1; fi
  rm -rf -- "$build_dir"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up -d --wait --wait-timeout 240
architecture=$(docker info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) goarch=arm64 ;;
  x86_64|amd64) goarch=amd64 ;;
  *) echo "Unsupported architecture: $architecture" >&2; exit 1 ;;
esac
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -o "$build_dir/quorumproof" ./tests/integration/quorumproof)
"${compose[@]}" cp "$build_dir/quorumproof" rabbitmq1:/tmp/quorumproof
"${compose[@]}" cp "$build_dir/quorumproof" rabbitmq2:/tmp/quorumproof
"${compose[@]}" exec -T rabbitmq1 /tmp/quorumproof seed
"${compose[@]}" exec -T rabbitmq1 rabbitmq-queues quorum_status rm.b0.quorum.node-loss
"${compose[@]}" kill --signal SIGKILL rabbitmq1
leader_id=$("${compose[@]}" ps -a -q rabbitmq1)
[[ $(docker inspect --format '{{.State.ExitCode}}' "$leader_id") == 137 ]] || {
  echo 'RabbitMQ leader SIGKILL was not observed' >&2
  exit 1
}
"${compose[@]}" kill --signal SIGKILL nsqd
nsq_id=$("${compose[@]}" ps -a -q nsqd)
[[ $(docker inspect --format '{{.State.ExitCode}}' "$nsq_id") == 137 ]] || {
  echo 'NSQ SIGKILL was not observed' >&2
  exit 1
}
"${compose[@]}" up -d --wait --wait-timeout 60 nsqd
"${compose[@]}" exec -T rabbitmq2 /tmp/quorumproof recover
"${compose[@]}" exec -T rabbitmq2 rabbitmq-queues quorum_status rm.b0.quorum.node-loss
"${compose[@]}" exec -T rabbitmq2 /tmp/quorumproof ack-seed
"${compose[@]}" exec -d rabbitmq2 sh -c '/tmp/quorumproof ack-take >/tmp/quorumproof-ack-take.log 2>&1'
"${compose[@]}" exec -T rabbitmq2 /tmp/quorumproof ack-inflight
echo 'PASS same original delivery in-flight without ACK/FIN on both brokers'
"${compose[@]}" exec -T rabbitmq2 sh -ec 'test -s /tmp/quorumproof-ack-take.pid; kill -KILL "$(cat /tmp/quorumproof-ack-take.pid)"'
redelivery_started=$(date +%s)
echo 'Consumer process SIGKILL completed; waiting for broker redelivery'
"${compose[@]}" exec -T rabbitmq2 /tmp/quorumproof ack-recover
echo "Consumer crash to both ACK/FIN drained: $(($(date +%s) - redelivery_started)) seconds"
