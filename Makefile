GO ?= go
GOLANGCI_VERSION := v2.5.0
.PHONY: check fmt-check test race vet lint
check: fmt-check vet test race
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
