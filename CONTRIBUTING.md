# Contributing

Use the Nix development shell (tool versions follow the locked `flake.lock`):

```sh
nix develop
just
```

`just lint` checks formatting, lint rules, and types across all SDKs.
`just check` runs their test suites. `just` runs lint and check together.
`just package` builds all package families below `dist/`.
`just package` requires a clean git tree (`cargo package` rejects uncommitted
changes); set `ZSTAR_ALLOW_DIRTY=1` to opt in to local experiments only.
The first run may download locked dependencies. RustFS checks are explicit:

```sh
just check-integration
```

The Nix shell provides `rustfs` on Linux only. On macOS, install a `rustfs`
binary yourself and put it on `PATH`, or rely on CI for `check-integration`.
`just package --manylinux` additionally needs a running Docker daemon and the
`docker` CLI, which the Nix shell does not provide.

The normative format is in `spec/`. Format changes require specification text,
positive and negative fixtures, and reader/writer coverage across SDKs. Keep
tests focused on behavior and resource limits; avoid tests that duplicate
implementation details. Do not commit build outputs or credentials.

Minimum toolchains are declared in [`Cargo.toml`](Cargo.toml) (Rust),
[`sdks/go/go.mod`](sdks/go/go.mod) (Go), and
[`sdks/python/pyproject.toml`](sdks/python/pyproject.toml) (Python).
The Nix shell fixes development tool versions through `flake.lock`; CI checks
the Rust and Python minimums and also tests with the shell or system Python.
See [`RELEASING.md`](RELEASING.md) for package release requirements.
