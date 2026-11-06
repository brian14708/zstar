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

Restart the development server after changing SDK READMEs or specifications so
it refreshes their generated pages. Rebuild to refresh the preview server's
static output. The pnpm workspace and lockfile live at the repository root.

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

starlight-pydocs generates `/zstar/api/python/` using locked Griffe static
analysis of `python/src/`. No native extension build or installed wheel is
needed. The reference follows public exports in `zstar` and `zstar.types`,
including the fsspec filesystem. Wire tuples and underscored helpers are private.
Extension values are public through `zstar.types.ExtensionValue` and
`EntryMetadata`. Site search includes the API, which also has symbol search.

The extraction adapter in `scripts/extract-python-api.py` resolves internal
generic mixin bases so inherited members appear in the reference.

The pinned starlight-pydocs patch in `patches/` gives top-level classes their
own pages, keeps the overview compact, and groups the class table of contents
by member kind. It also places constructor documentation on the class and
source links beside headings. When upgrading the plugin, review the patch
against its page model, HTML renderer, and Markdown output before updating
the pin.

## Deployment

Set the repository's Pages source to **GitHub Actions**. The
[Documentation workflow](../.github/workflows/docs.yml) builds pull requests
and deploys pushes or manual runs on `main` to
<https://brian14708.github.io/zstar/>.
