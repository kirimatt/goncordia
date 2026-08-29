# Contributing to Goncordia

Thank you for helping improve Goncordia. Bug reports, design feedback,
documentation, tests, and code contributions are welcome.

By participating, you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
Security vulnerabilities must be reported privately according to
[SECURITY.md](SECURITY.md), not through a public issue.

## Before opening an issue

- Search existing issues and pull requests for the same behavior or proposal.
- Reproduce bugs on the latest supported release when possible.
- Reduce bug reports to the smallest example that still fails.
- Include the Goncordia module/version, Go version, backend, and relevant
  configuration without credentials or production data.

Use the bug or feature issue form so the maintainers receive the information
needed to act on the report.

## Development setup

Goncordia requires Go 1.25 or newer. The repository uses `go.work` for local
development and independent `go.mod` files for drivers and integrations.
Docker is required for the complete backend integration suite.

```bash
git clone https://github.com/kirimatt/goncordia.git
cd goncordia
go version
./scripts/for-each-module.sh go mod tidy -diff
./scripts/for-each-module.sh go test ./... -count=1
```

The helper deliberately sets `GOWORK=off` inside every module. This catches
missing module requirements that a local workspace could otherwise hide.

For a targeted change, run commands from the affected module:

```bash
cd driver/pgxv5
GOWORK=off go test ./... -count=1
```

## Making changes

- Keep the root module dependency-light. Optional database SDKs and tooling
  belong in their existing nested modules.
- Preserve the public driver contracts and document compatibility changes.
- Use the injected `clock.Clock` and the manual clock in time-dependent tests;
  avoid wall-clock sleeps where deterministic control is possible.
- Add shared conformance coverage for behavior that every driver must support,
  plus backend-specific tests for storage guarantees and limitations.
- Keep migrations safe for existing users and document required rollout steps.
- Run `gofmt` on Go files and keep module manifests tidy.
- Update the README for user-visible behavior and the changelog for notable
  changes.

## Validation

Before submitting a pull request, run the checks relevant to the change. The
full local equivalent of CI is:

```bash
test -z "$(gofmt -l .)"
./scripts/for-each-module.sh go mod tidy -diff
./scripts/for-each-module.sh go vet ./...
./scripts/for-each-module.sh go test ./... -timeout 15m -race -count=1
./scripts/for-each-module.sh go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
./scripts/for-each-module.sh go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7
```

Container-backed tests may take several minutes. Pull requests must pass the
authoritative GitHub Actions matrix on all supported Go versions.

## Pull requests

Keep pull requests focused and explain:

- The problem and intended behavior.
- Public API, persistence, or compatibility effects.
- Which modules and backends are affected.
- How the change was tested.
- Any required migration or rollout steps.

Maintainers may ask for changes when a patch adds dependencies to the root
module, weakens a backend guarantee, relies on timing-sensitive tests, or lacks
cross-driver coverage.
