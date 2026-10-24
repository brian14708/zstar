set shell := ["bash", "-euo", "pipefail", "-c"]

python := `command -v python3`

default: lint check

lint: prepare-python
    cargo fmt --all -- --check
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
    cargo doc --locked --workspace --no-deps --all-features
    ruff check .
    ruff format --check .
    golangci-lint run ./...
    go vet ./...
    env -u PYTHONPATH uv run --no-sync python -I -m pyrefly check --min-severity=warn --config python/pyproject.toml --search-path python/src python/src/zstar
    env -u PYTHONPATH uv run --no-sync python -I -m pyrefly check --min-severity=warn --config python/pyproject.toml python/tests/typing_api.py

check: prepare-python
    cargo test --locked --workspace --all-features
    cargo test --locked -p zstar --no-default-features
    go test -race ./...
    uv run --no-sync python -I -m pytest python/tests

# Build all release candidates in dist/ from a clean git tree.
package *args: prepare-python
    python3 tools/release/package_go.py
    uv run --no-sync python -E -s tools/release/package_rust.py
    uv run --no-sync python -E -s tools/release/package_python.py {{ args }}

[private]
prepare-python:
    uv sync --locked --package zstar-py --group test --no-editable --python "{{ python }}"

check-integration: prepare-python
    #!/usr/bin/env bash
    set -euo pipefail
    # Own a disposable local service for this invocation; remove it even on failure.
    command -v rustfs >/dev/null
    # Refuse an occupied port so tests cannot accidentally use another service.
    python3 - <<'PY'
    import socket

    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 9000))
    PY
    rustfs_dir=$(mktemp -d "${TMPDIR:-/tmp}/zstar-rustfs.XXXXXX")
    rustfs_pid=""
    cleanup() {
        local status=$?
        if [[ -n "$rustfs_pid" ]]; then
            kill "$rustfs_pid" 2>/dev/null || true
            wait "$rustfs_pid" 2>/dev/null || true
        fi
        if (( status != 0 )); then
            cat "$rustfs_dir/server.log" >&2
        fi
        rm -rf "$rustfs_dir"
        exit "$status"
    }
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    (
        cd "$rustfs_dir"
        mkdir data
        export RUSTFS_ACCESS_KEY=zstar-test RUSTFS_SECRET_KEY=zstar-test-secret RUSTFS_CONSOLE_ENABLE=false
        exec rustfs server --address 127.0.0.1:9000 "$rustfs_dir/data"
    ) >"$rustfs_dir/server.log" 2>&1 &
    rustfs_pid=$!
    export ZSTAR_S3_ENDPOINT=http://127.0.0.1:9000
    rustfs_ready=false
    for ((attempt = 0; attempt < 60; attempt++)); do
        if ! kill -0 "$rustfs_pid" 2>/dev/null; then
            echo "RustFS exited before becoming ready" >&2
            exit 1
        fi
        if curl --fail --silent --connect-timeout 1 --max-time 2 "$ZSTAR_S3_ENDPOINT/health/ready" >/dev/null; then
            rustfs_ready=true
            break
        fi
        sleep 1
    done
    if [[ "$rustfs_ready" != true ]]; then
        echo "Timed out waiting for RustFS" >&2
        exit 1
    fi
    go test -race -tags=integration ./...
    cargo test --locked -p zstar --features s3 --test rustfs -- --ignored
    # Test caller-installed S3 chaining in an isolated environment; keep the base install free of s3fs.
    uv run --no-sync --with "s3fs>=2026.9.0" python -I -m pytest python/tests
