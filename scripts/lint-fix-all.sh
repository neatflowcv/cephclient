#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/go-cache}"
export GOLANGCI_LINT_CACHE="${GOLANGCI_LINT_CACHE:-/tmp/golangci-lint-cache}"

# Run against the whole repository, not only changed files.
golangci-lint fmt
golangci-lint run --fix
golangci-lint fmt
