#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
project="rm-sub-$(date +%s)-$$-${RANDOM}"
compose=(docker compose --project-name "$project" --file "$repo/tests/integration/compose.yaml" --file "$repo/tests/integration/compose-subscription.yaml")
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

"${compose[@]}" up -d --wait --wait-timeout 180 mysql nsqlookupd nsqd
architecture=$(docker info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) goarch=arm64 ;;
  x86_64|amd64) goarch=amd64 ;;
  *) echo "Unsupported architecture: $architecture" >&2; exit 1 ;;
esac
(cd "$repo" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go test -c -tags=integration -o "$build_dir/nsq-subscription-test" ./tests/integration)
"${compose[@]}" cp "$build_dir/nsq-subscription-test" mysql:/tmp/nsq-subscription-test
"${compose[@]}" cp nsqd:/usr/local/bin/nsqd "$build_dir/nsqd-peer"
"${compose[@]}" cp "$build_dir/nsqd-peer" mysql:/tmp/nsqd-peer
"${compose[@]}" exec -T mysql mysql -uroot -e 'CREATE DATABASE rm_failure_audit_test'
test_pattern=${RM_TEST_RUN:-'^TestNSQ(SubscriptionTerminalHandoffLostConfirmation|SubscriberOwnsConsumersAndTerminalHandoff|SubscriberLookupdTopology|SubscriberLookupdOutageWithConnectedBroker|SharedFailureGroupSurvivesEphemeralSubscriberReplacement|SubscriberBeforeTopicRegistration|SubscriberDurableFailureAudit|RawPublisherPreservesLegacyEnvelope|ProvisionedFirstMessageBeforeConsumer|SubscriberLateNodeKillAndRejoin)$'}
"${compose[@]}" exec -T -e RM_TEST_MYSQL_DSN='root@tcp(127.0.0.1:3306)/rm_failure_audit_test?parseTime=true&loc=UTC' -e RM_TEST_NSQ_TCP='nsqd:4150' -e RM_TEST_NSQ_HTTP='http://nsqd:4151' -e RM_TEST_NSQ_LOOKUPD='nsqlookupd:4161' -e RM_TEST_NSQD_BINARY='/tmp/nsqd-peer' mysql /tmp/nsq-subscription-test -test.v -test.run "$test_pattern"
