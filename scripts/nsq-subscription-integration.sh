#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
project="rm-sub-$(date +%s)-$$-${RANDOM}"
compose=(docker compose --project-name "$project" --file "$repo/tests/integration/compose.yaml")
context=$(docker context show)
endpoint=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}')
if [[ -n ${DOCKER_HOST:-} || "$endpoint" != unix://* ]]; then
  echo "NSQ subscription test requires a local Unix-socket Docker context" >&2
  exit 1
fi
docker info >/dev/null
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/$project-build.XXXXXX")
cleanup() {
  result=$?
  trap - EXIT
  if ((result != 0)); then "${compose[@]}" logs --no-color --tail 40 || true; fi
  if ! "${compose[@]}" down --volumes --remove-orphans --timeout 10; then result=1; fi
  rm -rf -- "$build_dir"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up -d --wait --wait-timeout 180 mysql nsqd
architecture=$(docker info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) goarch=arm64 ;;
  x86_64|amd64) goarch=amd64 ;;
  *) echo "Unsupported architecture: $architecture" >&2; exit 1 ;;
esac
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go test -c -tags=integration -o "$build_dir/nsq-subscription-test" ./tests/integration)
"${compose[@]}" cp "$build_dir/nsq-subscription-test" mysql:/tmp/nsq-subscription-test
"${compose[@]}" exec -T -e RM_TEST_NSQ_TCP='nsqd:4150' mysql /tmp/nsq-subscription-test -test.v -test.run '^TestNSQ(SubscriptionTerminalHandoffLostConfirmation|SubscriberOwnsConsumersAndTerminalHandoff)$'
