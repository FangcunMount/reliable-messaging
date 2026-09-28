#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
# No externally supplied project name, URI, volumes or production config.
project="rm-test-$(date +%s)-$$-${RANDOM}"
nsq_stats="${TMPDIR:-/tmp}/$project-nsq.json"
compose=(docker compose --project-name "$project" --file "$repo/tests/integration/compose.yaml" --file "$repo/tests/integration/compose-subscription.yaml")
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
"${compose[@]}" exec -T mongo mongosh --quiet --file /dev/stdin < "$repo/tests/integration/mongo-smoke.js"
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go test -c -tags=integration -o "$build_dir/mysql-integration" ./tests/integration)
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go test -c -tags=integration -o "$build_dir/nsq-adapter-integration" ./transport/nsq)
"${compose[@]}" cp "$build_dir/mysql-integration" mysql:/tmp/mysql-integration
"${compose[@]}" cp "$build_dir/nsq-adapter-integration" mysql:/tmp/nsq-adapter-integration
"${compose[@]}" cp nsqd:/usr/local/bin/nsqd "$build_dir/nsqd-peer"
"${compose[@]}" cp "$build_dir/nsqd-peer" mysql:/tmp/nsqd-peer
"${compose[@]}" exec -T mysql mysql -uroot -e 'CREATE DATABASE rm_sdk_test'
"${compose[@]}" exec -T -e RM_TEST_MYSQL_DSN='root@tcp(127.0.0.1:3306)/rm_sdk_test?parseTime=true&loc=UTC' -e RM_TEST_MONGO_URI='mongodb://mongo:27017/?replicaSet=rm-test' -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' -e RM_TEST_NSQ_LOOKUPD='nsqlookupd:4161' -e RM_TEST_NSQD_BINARY='/tmp/nsqd-peer' mysql /tmp/mysql-integration -test.v
"${compose[@]}" exec -T -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' mysql /tmp/nsq-adapter-integration -test.v -test.run '^Test(DirectHandoffRealDisconnectRetainsIdentityAndDrains|ManagedPublisherOwnsRealNSQConnection|ManagedPublisherInterruptsRealUnconfirmedSend)$'

build_released_handoff() {
  local tag=$1 expected=$2 label=$3 source_dir="$build_dir/handoff-$3"
  if ! git -C "$repo" cat-file -e "refs/tags/$tag^{commit}" 2>/dev/null; then
    git -C "$repo" fetch --no-tags --depth=1 origin "refs/tags/$tag:refs/tags/$tag"
  fi
  [[ $(git -C "$repo" rev-parse "$tag^{commit}") == "$expected" ]] || {
    echo "Released tag $tag did not resolve to the approved commit" >&2
    exit 1
  }
  mkdir -p "$source_dir/versionhandoff"
  git -C "$repo" archive "$tag" | tar -xf - -C "$source_dir"
  cp "$repo/tests/integration/versionhandoff/"*.go "$source_dir/versionhandoff/"
  (cd "$source_dir" && GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -o "$build_dir/handoff-$label.bin" ./versionhandoff)
  "${compose[@]}" cp "$build_dir/handoff-$label.bin" "mysql:/tmp/handoff-$label"
}

# Build the same host scenario against two actual release tags. A current
# Appender on a hand-edited old schema would not prove binary-version handoff.
build_released_handoff v0.1.0 1cab5531ee985fff9561b94c9b3d396230f870d4 old
build_released_handoff v0.2.1 5bbbacb15f9c044e7ef27d4384110a97b92c745f new
"${compose[@]}" exec -T mysql mysql -uroot -e 'CREATE DATABASE rm_sdk_version_handoff'
handoff_dsn='root@tcp(127.0.0.1:3306)/rm_sdk_version_handoff?parseTime=true&loc=UTC'
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-old old-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new new-before-ddl
"${compose[@]}" exec -T mysql mysql -uroot rm_sdk_version_handoff -e 'ALTER TABLE rm_outbox ADD COLUMN failure_count BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER attempt_count, ADD COLUMN updated_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)) AFTER created_at'
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-old old-after-ddl
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new new-drain
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-old old-drain
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-old old-retry-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new new-retry
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-old old-retry-downgrade
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new new-retry-quarantine
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-old old-lease-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new new-lease-recover
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new new-lease-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-old old-lease-recover
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new parallel-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_PARTICIPANT=old mysql /tmp/handoff-old parallel-claim > "$build_dir/mysql-parallel-old.log" 2>&1 &
mysql_old_pid=$!
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_PARTICIPANT=new mysql /tmp/handoff-new parallel-claim > "$build_dir/mysql-parallel-new.log" 2>&1 &
mysql_new_pid=$!
mysql_old_status=0
mysql_new_status=0
wait "$mysql_old_pid" || mysql_old_status=$?
wait "$mysql_new_pid" || mysql_new_status=$?
if ((mysql_old_status != 0 || mysql_new_status != 0)); then
  cat "$build_dir/mysql-parallel-old.log" "$build_dir/mysql-parallel-new.log" >&2
  exit 1
fi
cat "$build_dir/mysql-parallel-old.log" "$build_dir/mysql-parallel-new.log"
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" mysql /tmp/handoff-new parallel-verify
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new relay-parallel-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' -e RM_HANDOFF_PARTICIPANT=old mysql /tmp/handoff-old relay-parallel-run > "$build_dir/mysql-relay-old.log" 2>&1 &
mysql_relay_old_pid=$!
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' -e RM_HANDOFF_PARTICIPANT=new mysql /tmp/handoff-new relay-parallel-run > "$build_dir/mysql-relay-new.log" 2>&1 &
mysql_relay_new_pid=$!
mysql_relay_old_status=0
mysql_relay_new_status=0
wait "$mysql_relay_old_pid" || mysql_relay_old_status=$?
wait "$mysql_relay_new_pid" || mysql_relay_new_status=$?
if ((mysql_relay_old_status != 0 || mysql_relay_new_status != 0)); then
  cat "$build_dir/mysql-relay-old.log" "$build_dir/mysql-relay-new.log" >&2
  exit 1
fi
cat "$build_dir/mysql-relay-old.log" "$build_dir/mysql-relay-new.log"
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new relay-parallel-verify
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new lost-ack-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-old lost-ack-old
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new lost-ack-new
"${compose[@]}" exec -T -e RM_HANDOFF_MYSQL_DSN="$handoff_dsn" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new lost-ack-verify
handoff_mongo_uri='mongodb://mongo:27017/?replicaSet=rm-test'
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-old mongo-old-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-new-index
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-old mongo-old-after-index
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-new-drain
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-old mongo-old-drain
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-old mongo-old-retry-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-new-retry
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-old mongo-old-retry-downgrade
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-new-retry-quarantine
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-old mongo-old-lease-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-new-lease-recover
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-new-lease-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-old mongo-old-lease-recover
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-parallel-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_PARTICIPANT=old mysql /tmp/handoff-old mongo-parallel-claim > "$build_dir/mongo-parallel-old.log" 2>&1 &
mongo_old_pid=$!
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_PARTICIPANT=new mysql /tmp/handoff-new mongo-parallel-claim > "$build_dir/mongo-parallel-new.log" 2>&1 &
mongo_new_pid=$!
mongo_old_status=0
mongo_new_status=0
wait "$mongo_old_pid" || mongo_old_status=$?
wait "$mongo_new_pid" || mongo_new_status=$?
if ((mongo_old_status != 0 || mongo_new_status != 0)); then
  cat "$build_dir/mongo-parallel-old.log" "$build_dir/mongo-parallel-new.log" >&2
  exit 1
fi
cat "$build_dir/mongo-parallel-old.log" "$build_dir/mongo-parallel-new.log"
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" mysql /tmp/handoff-new mongo-parallel-verify
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new mongo-relay-parallel-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' -e RM_HANDOFF_PARTICIPANT=old mysql /tmp/handoff-old mongo-relay-parallel-run > "$build_dir/mongo-relay-old.log" 2>&1 &
mongo_relay_old_pid=$!
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' -e RM_HANDOFF_PARTICIPANT=new mysql /tmp/handoff-new mongo-relay-parallel-run > "$build_dir/mongo-relay-new.log" 2>&1 &
mongo_relay_new_pid=$!
mongo_relay_old_status=0
mongo_relay_new_status=0
wait "$mongo_relay_old_pid" || mongo_relay_old_status=$?
wait "$mongo_relay_new_pid" || mongo_relay_new_status=$?
if ((mongo_relay_old_status != 0 || mongo_relay_new_status != 0)); then
  cat "$build_dir/mongo-relay-old.log" "$build_dir/mongo-relay-new.log" >&2
  exit 1
fi
cat "$build_dir/mongo-relay-old.log" "$build_dir/mongo-relay-new.log"
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new mongo-relay-parallel-verify
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new mongo-lost-ack-seed
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-old mongo-lost-ack-old
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new mongo-lost-ack-new
"${compose[@]}" exec -T -e RM_HANDOFF_MONGO_URI="$handoff_mongo_uri" -e RM_HANDOFF_NSQ_TCP='nsqd:4150' -e RM_HANDOFF_NSQ_HTTP='http://nsqd:4151' mysql /tmp/handoff-new mongo-lost-ack-verify




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
# Restart only the dedicated broker; the per-project volume survives this stop
# and is removed by the existing cleanup trap. MySQL/Mongo remain running.
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -o "$build_dir/broker-restart" ./tests/integration/brokerrestart)
"${compose[@]}" cp "$build_dir/broker-restart" mysql:/tmp/broker-restart
"${compose[@]}" exec -T -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' mysql /tmp/broker-restart seed /tmp/rm-restart-manifest.json rm-restart
"${compose[@]}" stop --timeout 15 nsqd
broker_id=$("${compose[@]}" ps -a -q nsqd)
[[ $(docker inspect --format '{{.State.ExitCode}}' "$broker_id") == 0 ]] || { echo 'Broker did not stop gracefully' >&2; exit 1; }
"${compose[@]}" up -d --wait --wait-timeout 60 nsqd
"${compose[@]}" exec -T -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' mysql /tmp/broker-restart recover /tmp/rm-restart-manifest.json rm-restart
# Characterize the weaker abrupt-crash boundary separately. This intentionally
# verifies loss of confirmed memory-only messages, not successful recovery.
"${compose[@]}" exec -T -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' mysql /tmp/broker-restart seed /tmp/rm-crash-manifest.json rm-crash
"${compose[@]}" exec -T -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' mysql /tmp/broker-restart buffered /tmp/rm-crash-manifest.json rm-crash
"${compose[@]}" kill --signal SIGKILL nsqd
[[ $(docker inspect --format '{{.State.ExitCode}}' "$broker_id") == 137 ]] || { echo 'Broker SIGKILL was not observed' >&2; exit 1; }
"${compose[@]}" up -d --wait --wait-timeout 60 nsqd
"${compose[@]}" exec -T -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' mysql /tmp/broker-restart lost /tmp/rm-crash-manifest.json rm-crash
echo 'PASS implemented tests; abrupt broker crash durability is NOT guaranteed and the full fault matrix remains incomplete'
