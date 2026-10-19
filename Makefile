GO ?= go
GOLANGCI_VERSION := v2.5.0
.PHONY: check docs-check fmt-check test race vet lint integration subscription-integration
check: docs-check fmt-check rocketmq-source-check vet test race
.PHONY: rocketmq-source-check
rocketmq-source-check:
	python3 scripts/check_rocketmq_driver.py
docs-check:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_check_docs.py
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_docs.py
fmt-check:
	@test -z "$$(gofmt -l $$(git ls-files '*.go') $$(git ls-files --others --exclude-standard '*.go'))"
vet:
	$(GO) vet ./...
test:
	$(GO) test -count=1 ./...
race:
	$(GO) test -race -count=1 ./...
lint:
	GOBIN="$(CURDIR)/bin" $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
	./bin/golangci-lint run
integration:
	bash scripts/integration.sh
subscription-integration:
	bash scripts/nsq-subscription-integration.sh
.PHONY: transaction-storage-integration
transaction-storage-integration:
	python3 scripts/transaction-storage-integration.py --sdk "$(CURDIR)" --report artifacts/transaction-storage.json
