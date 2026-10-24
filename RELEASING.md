# Version and release policy

The wire version, corpus manifest schema, and SDK package versions are separate.
An SDK release must declare supported wire versions and required features.
A package version change never silently changes wire semantics. Changes that
make existing valid v1 archives unreadable require a new wire version or an
explicit required feature, together with reviewed specification and corpus updates.

SDKs use semantic versioning independently. During 0.x development, breaking
public API changes increment the minor version; patches contain compatible
fixes. After 1.0, incompatible public API changes require a major version.
Rust workspace crates currently share a version; Python and Go packages
can release independently. Never reuse an existing release version or tag.

Use package-scoped tags:

| Package | Tag | Distribution |
| --- | --- | --- |
| Rust workspace | `rust/vX.Y.Z` | crates.io |
| Python | `python/vX.Y.Z` | PyPI wheels and source distribution |
| Go module | `sdks/go/vX.Y.Z` | Git tag, matching the module subdirectory |

Go v2+ also requires the `/v2` module import path and matching module directory
conventions; a tag alone does not establish a new major module.

Before a release:

1. Update the package version, lockfiles (including the `zstar` entry in
   `[workspace.dependencies]` of `Cargo.toml`, whose `version` must match
   the crate version being published), changelog/release notes, and supported
   platforms. Record user-visible changes and known limitations.
2. Run the full SDK/corpus matrix, archive exchange, and
   local RustFS integration. Build Python wheels on Linux/macOS and test the
   installed artifacts.
3. Build from the reviewed commit in a clean checkout using the locked Nix shell
   and pinned build tools (the workflow rejects a dirty tree). Check
   package contents, licenses, generated assets, source inclusion and secret
   exclusion; verify installation and a reader/writer round trip in isolation.
4. Publish immutable artifacts using registry trusted publishing/OIDC and
   minimum job permissions. Attach checksums, source commit, build provenance,
   and available registry attestations. Sign tags using the maintainer's
   configured signing identity.
5. Verify the published artifacts with a fresh install and link their release
   notes, compatibility declaration, and provenance.

The `Package candidates` workflow builds Rust and Python artifacts
and requests GitHub build-provenance attestations. Trigger it manually or with a
package-scoped version tag; tags must match the selected manifest version.
Run `nix develop --command just package` to build all package families
(requires a clean tree; `ZSTAR_ALLOW_DIRTY=1` opts in to local experiments).
Outputs and SHA256SUMS are written below `dist/`. The release workflow invokes
each package builder directly for the selected family.

Python packaging uses `uv build` through `tools/release/package_python.py`;
uv installs the Maturin backend declared in `pyproject.toml`. `--manylinux` selects
the Docker build. Portable build settings live in `sdks/python/pyproject.toml`;
the release workflow tests installed candidate wheels with pytest. The script
writes checksums after successful builds.

Python candidates build wheels from the source distribution; the release
workflow tests installed wheels with the SDK suite. Linux candidates use
cibuildwheel's pinned manylinux_2_28 images on native x86_64 and aarch64 runners (Rust 1.97.0 and rustup-init 1.28.2 are
pinned, the installer by SHA-256), audit the cp311 abi3 wheel, and install and
exercise that wheel on the system Python. Run the Linux path locally with
`nix develop --command just package --manylinux` (Docker required).
The native `just package` command produces host-ABI wheels; Linux output from
that command must not be published as portable wheels. macOS candidates use the
native command on arm64 and x86_64 runners and run the same abi3 audit.
The source distribution is built, verified and attested once by a dedicated job;
wheel jobs build from their own sdist build and publish wheels only. Windows x86_64 candidates build with
Rust 1.97 from the extracted source distribution, audit the stable ABI, and run
the full installed-wheel suite on the system Python. macOS candidates also test
the packaged artifact on the system Python. Rust candidates compile each packaged crate,
resolving unpublished sibling dependencies through Cargo's temporary registry.

Registry publishing, hosted attestation verification, and execution evidence
for the complete release matrix remain pending. Current CI does not publish
packages. Do not describe development builds as beta-ready until the full CI
and release matrix has passed.
