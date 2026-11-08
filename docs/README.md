# Documentation

Run all commands below from the repository root.

## Build and preview

```sh
nix develop --command just docs
nix develop --command pnpm --filter zstar-docs run preview
```

The build installs locked Python and pnpm dependencies and writes the site to
`docs/dist`. Preview it at `http://localhost:4321/zstar/`.

For live editing after the initial build:

```sh
nix develop --command pnpm --filter zstar-docs run dev
```

Restart the development server after changing SDK READMEs or specifications.
Rebuild to refresh the preview server's static output.

## Edit content

| Content | Source |
| --- | --- |
| SDK overviews | Package READMEs in `python/`, `crates/zstar/`, and `go/` |
| Specifications | `spec/draft/` |
| Tutorials | `src/content/docs/tutorials/` |
| Advanced usage guides | `src/content/docs/guides/` |
| Python API reference | Public exports and docstrings in `python/src/` |
| Reading styles | `src/styles/typography.css` |
| Palette and API styles | `src/styles/theme.css` |

Paths beginning with `src/` are relative to `docs/`. The site copies SDK READMEs
and specifications into `src/content/docs/sdks/` and `src/content/docs/reference/`
before each build or development-server start. Edit the sources, not those
generated copies.

Tutorials import runnable examples from each SDK's examples directory. The
`Example` component renders whole files or named regions, so excerpts stay in
sync with the programs. Missing regions fail the site build.

## Python API generation

starlight-pydocs generates `/zstar/api/python/` from `python/src/` using locked
Griffe static analysis. It follows public exports in `zstar`, `zstar.types`, and the `sources`, `sinks`,
`fs`, `tar`, and optional `fsspec` modules,
including the fsspec filesystem, without a native build or installed wheel.

The extraction adapter in `scripts/extract-python-api.py` resolves internal
generic mixin bases so inherited members appear in the reference.

The pinned patch in `patches/` customizes API pages and navigation. Review it
against the plugin's page model, HTML renderer, and Markdown output when upgrading.

## Deployment

Set the repository's Pages source to **GitHub Actions**. The
[Documentation workflow](../.github/workflows/docs.yml) builds pull requests
and deploys pushes or manual runs on `main` to
<https://brian14708.github.io/zstar/>.
