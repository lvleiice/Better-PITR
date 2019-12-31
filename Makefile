### Makefile for tidb-binlog
.PHONY: build test check update clean pitr fmt

PROJECT=tidb-binlog

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
FAIL_ON_STDOUT := awk '{ print  } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

TEST_DIR := /tmp/pitr_test

GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3

ARCH  := "`uname -s`"
LINUX := "Linux"
MAC   := "Darwin"
PACKAGE_LIST := go list ./...| grep -vE 'vendor|proto'
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES := $$(find . -name '*.go' -type f)

default: build buildsucc

buildsucc:
	@echo Build PITR Utils successfully!

all: dev install

dev: check test

build: pitr  

pitr:
	$(GOBUILD) -o bin/pitr ./cmd/main.go

install:
	go install ./...

unit_test:
	mkdir -p "$(TEST_DIR)"
	@export log_level=error;\
	$(GOTEST) -cover -covermode=count -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES)

integration_test:
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/sync_diff_inspector
	@which bin/drainer
	@which bin/pump
	@which bin/reparo
	sh tests/run.sh

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

vet:
	@echo "vet"
	$(GO) vet $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

check: fmt lint check-static tidy

coverage:
	GO111MODULE=off go get github.com/wadey/gocovmerge
	gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go" > "$(TEST_DIR)/all_cov.out"
ifeq ("$(JenkinsCI)", "1")
	GO111MODULE=off go get github.com/mattn/goveralls
	@goveralls -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
	grep -F '<option' "$(TEST_DIR)/all_cov.html"
endif

check-static: tools/bin/golangci-lint
	$(GO) mod vendor
	tools/bin/golangci-lint --disable errcheck run $$($(PACKAGE_DIRECTORIES))

clean:
	go clean -i ./...
	rm -rf *.out

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/golangci-lint: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint
