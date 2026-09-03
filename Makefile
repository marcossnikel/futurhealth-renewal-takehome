.PHONY: setup fmt test check workflowcheck

WORKFLOWCHECK := go.temporal.io/sdk/contrib/tools/workflowcheck@v0.5.0

setup:
	go mod download
	go mod download $(WORKFLOWCHECK)

fmt:
	gofmt -w ./cmd ./internal ./pkg

test:
	go test ./...

workflowcheck:
	go run $(WORKFLOWCHECK) ./...

check:
	@test -z "$$(gofmt -l ./cmd ./internal ./pkg)"
	go vet ./...
	go test -race ./...
	$(MAKE) workflowcheck
