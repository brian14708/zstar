# Contributing

## Development environment

Run commands from the repository root unless a command changes directory.
Use the Nix development shell for the tool versions pinned by `flake.lock`:

```sh
nix develop
just
```

Minimum supported versions are declared in [Cargo.toml](Cargo.toml) (Rust),
[go.mod](go.mod) (Go), and [python/pyproject.toml](python/pyproject.toml)
(Python). CI checks the Rust and Python minimums as well as the development
shell versions. The first run may download locked dependencies.

## Checks

| Command | Checks |
| --- | --- |
| `just` | All lint and standard test checks |
| `just lint` | Rust formatting, Clippy, and docs; Python Ruff and Pyrefly; Go lint and vet |
| `just check` | Rust tests with all features and without defaults, Go race tests, and Python pytest |
| `just check-integration` | Disposable local RustFS integration tests across the SDKs |

The recipes build and install a Python wheel into the uv environment before
checking Python. `just check` exercises the installed package rather than an
editable source checkout. Integration tests additionally install `s3fs` in an
isolated environment for fsspec chaining.

RustFS checks require a `rustfs` binary on `PATH` and an available local port
9000. The Nix shell provides RustFS on Linux. On macOS, install it separately
or use the Linux integration job in CI. The recipe starts and cleans up its own
server and temporary data.

For focused checks:

```sh
cargo test --locked -p zstar --all-features
cargo test --locked -p zstar-cli
uv run --no-sync python -I -m pytest python/tests
```

Run the Python command after `just check` or `just lint` has prepared the wheel.
For Go, run `go test -race ./...` from the repository root.

## Documentation and format changes

The normative wire format and implementation profiles are in
[spec/v1.md](spec/v1.md). Format changes require specification updates, positive
and negative fixtures under `testdata/`, and reader/writer coverage across SDKs.
Keep tests focused on observable behavior and resource limits.

Update the relevant package README and public item documentation when changing
behavior. Rust module and item docs are part of rustdoc; check their examples
with `cargo test --locked -p zstar --all-features --doc`.

Use concise, imperative commit subjects such as `docs: clarify reader limits`.
PRs should describe the problem, resulting behavior, affected SDKs, and validation
commands, and link relevant issues. Do not commit build outputs or credentials.

## Packaging

```sh
just package
```

This builds Rust crates, a Go module source archive, and Python distributions
under `dist/`. Run it from a clean Git tree. The Go packager archives `HEAD` and
rejects a dirty tree. `ZSTAR_ALLOW_DIRTY=1` permits local experiments with the
Rust packager only; it does not bypass the Go packager's check.

`just package --manylinux` also requires a running Docker daemon and the
`docker` CLI, which the Nix shell does not provide.
The [Packages workflow](.github/workflows/packages.yml) builds, tests, attests,
and uploads package artifacts on version tags or manual dispatch. Root `vX.Y.Z`
tags build all SDKs and publish Rust and Python packages. The root Go module
uses the same `vX.Y.Z` tags; its source archive includes `go/`, shared
`testdata/`, and the root module files.
