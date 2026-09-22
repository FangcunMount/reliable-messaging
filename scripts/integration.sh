#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
# No externally supplied project name, URI, volumes or production config.
project="rm-test-$(date +%s)-$$-${RANDOM}"
nsq_stats="${TMPDIR:-/tmp}/$project-nsq.json"
compose=(docker compose --project-name "$project" --file "$repo/tests/integration/compose.yaml")
context=$(docker context show)
endpoint=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}')
if [[ -n ${DOCKER_HOST:-} || "$endpoint" != unix://* ]]; then
  echo "Integration requires a local Unix-socket Docker context without DOCKER_HOST" >&2
  exit 1
fi
docker info >/dev/null
docker compose version
cleanup() {
  result=$?
  trap - EXIT
  rm -f -- "$nsq_stats"
  if [[ -n ${build_dir:-} ]]; then rm -rf -- "$build_dir"; fi
  if ((result != 0)); then "${compose[@]}" logs --no-color --tail 40 || true; fi
  if ! "${compose[@]}" down --volumes --remove-orphans --timeout 10; then result=1; fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${compose[@]}" up -d --wait --wait-timeout 180

mysql_result=$("${compose[@]}" exec -T mysql mysql -uroot -N < "$repo/tests/integration/mysql-smoke.sql")
[[ "$mysql_result" == 'PASS MySQL commit/rollback' ]] || { echo "$mysql_result" >&2; exit 1; }
echo "$mysql_result"
"${compose[@]}" exec -T mysql mysql -uroot -N -e 'SELECT VERSION()'

architecture=$(docker info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) goarch=arm64 ;;
  x86_64|amd64) goarch=amd64 ;;
  *) echo "Unsupported example architecture: $architecture" >&2; exit 1 ;;
esac
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/$project-build.XXXXXX")
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -o "$build_dir/transaction-example" ./examples/transactional-publisher)
"${compose[@]}" cp "$build_dir/transaction-example" mysql:/tmp/transaction-example
"${compose[@]}" exec -T mysql mysql -uroot -e 'CREATE DATABASE rm_example_test'
"${compose[@]}" exec -T -e RM_EXAMPLE_MYSQL_DSN='root@tcp(127.0.0.1:3306)/rm_example_test' mysql /tmp/transaction-example
(cd "$repo" && go run ./examples/host-lifecycle)
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go test -c -tags=integration -o "$build_dir/mysql-integration" ./tests/integration)
"${compose[@]}" cp "$build_dir/mysql-integration" mysql:/tmp/mysql-integration
"${compose[@]}" exec -T mysql mysql -uroot -e 'CREATE DATABASE rm_sdk_test'
"${compose[@]}" exec -T -e RM_TEST_MYSQL_DSN='root@tcp(127.0.0.1:3306)/rm_sdk_test?parseTime=true&loc=UTC' -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' mysql /tmp/mysql-integration -test.v


"${compose[@]}" exec -T mongo mongosh --quiet --file /dev/stdin < "$repo/tests/integration/mongo-smoke.js"

"${compose[@]}" exec -T nsqd sh -ec '
  wget -qO- --post-data="" "http://127.0.0.1:4151/topic/create?topic=rm-smoke"
  wget -qO- --post-data="" "http://127.0.0.1:4151/channel/create?topic=rm-smoke&channel=rm-smoke"
  wget -qO- --post-data="fixture" "http://127.0.0.1:4151/pub?topic=rm-smoke"
  wget -qO- "http://127.0.0.1:4151/stats?format=json"
' > "$nsq_stats"
# wget responses to successful POSTs are empty except publish's OK.
python3 - "$nsq_stats" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
try:
    raw = path.read_text()
    stats = json.loads(raw[raw.index('{'):])
    topic = next(t for t in stats['topics'] if t['topic_name'] == 'rm-smoke')
    channel = next(c for c in topic['channels'] if c['channel_name'] == 'rm-smoke')
    assert topic['message_count'] == 1, topic
    assert channel['depth'] == 1, channel
    print('PASS NSQ publish and queued channel (not consumer ACK proof)')
finally:
    path.unlink(missing_ok=True)
PY
echo 'PASS isolated infrastructure and implemented SDK tests; full fault matrix remains incomplete'
