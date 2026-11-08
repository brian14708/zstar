import starlight from '@astrojs/starlight';
import { defineConfig } from 'astro/config';
import starlightPydocs, { pydocsSidebarGroup } from 'starlight-pydocs';

export default defineConfig({
  site: 'https://brian14708.github.io',
  base: '/zstar/',
  integrations: [
    starlight({
      title: 'Zstar',
      plugins: [
        starlightPydocs({
          components: { Signature: './src/components/ApiSignature.astro' },
          runner: { command: ['uv', 'run', '--no-sync', 'python', '-I', 'scripts/extract-python-api.py'] },
          packages: [{
            name: 'zstar',
            base: 'api/python',
            label: 'Python API',
            // Track the extraction adapter in the plugin's dump cache key too.
            search: ['../python/src', './scripts'],
            docstringStyle: 'sphinx',
            filters: { special: true },
            members: {
              include: [
                'zstar.open',
                'zstar.Archive', 'zstar.Archive.**',
                'zstar.ArchiveFile', 'zstar.ArchiveFile.**',
                'zstar.AsyncArchiveFile', 'zstar.AsyncArchiveFile.**',
                'zstar.AsyncArchive', 'zstar.AsyncArchive.**',
                'zstar.ArchiveWriter', 'zstar.ArchiveWriter.**',
                'zstar.AsyncArchiveWriter', 'zstar.AsyncArchiveWriter.**',
                'zstar.ZstarError', 'zstar.ZstarError.**',
                'zstar.types', 'zstar.types.**',
                'zstar.Entry', 'zstar.Entry.**',
                'zstar.Header', 'zstar.Header.**',
                'zstar.Extent', 'zstar.Extent.**',
                'zstar.EntryKind', 'zstar.EntryKind.**',
                'zstar.*Options', 'zstar.*Options.**',
                'zstar.Credentials', 'zstar.Credentials.**',
                'zstar.Limits', 'zstar.ReadRequest',
                'zstar.tar', 'zstar.tar.**',
                'zstar.fsspec', 'zstar.fsspec.**',
              ],
            },
            sourceLink: { host: 'github', repo: 'brian14708/zstar', root: '..' },
          }],
        }),
      ],
      logo: { src: './public/logo.svg', alt: '' },
      description: 'A core specification for indexed tar.zst archives, with optional extensions.',
      customCss: ['./src/styles/typography.css', './src/styles/theme.css'],
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/brian14708/zstar' }],
      expressiveCode: { shiki: { langAlias: { cddl: 'text', abnf: 'text' } } },
      sidebar: [
        {
          label: 'Start here',
          items: [
            { label: 'Overview', link: '/' },
            { label: 'Getting started', link: '/getting-started/' },
            { label: 'SDK customization', link: '/guides/sdk-customization/' },
          ],
        },
        {
          label: 'Python',
          items: [
            { label: 'Overview', link: '/sdks/python/' },
            { label: 'Create your first archive', link: '/tutorials/python/first-archive/' },
            { label: 'Advanced archive creation', link: '/tutorials/python/advanced-archive/' },
            { label: 'Read from S3', link: '/tutorials/python/s3/' },
            { label: 'Advanced usage', link: '/guides/python/configuration/' },
          ],
        },
        {
          label: 'Rust',
          items: [
            { label: 'Overview', link: '/sdks/rust/' },
            { label: 'Create your first archive', link: '/tutorials/rust/first-archive/' },
            { label: 'Blocking I/O', link: '/guides/rust/blocking/' },
            { label: 'Read from S3', link: '/tutorials/rust/s3/' },
            { label: 'Advanced usage', link: '/guides/rust/configuration/' },
          ],
        },
        {
          label: 'Go',
          items: [
            { label: 'Overview', link: '/sdks/go/' },
            { label: 'Create your first archive', link: '/tutorials/go/first-archive/' },
            { label: 'Read from S3', link: '/tutorials/go/s3/' },
            { label: 'Advanced usage', link: '/guides/go/configuration/' },
          ],
        },
        {
          label: 'Reference',
          items: [
            {
              label: 'Draft specifications',
              items: [
                { label: 'Overview', link: '/reference/spec/' },
                { label: 'Core format', link: '/reference/spec/core/' },
                { label: 'External content', link: '/reference/spec/external-content/' },
                { label: 'Catalog proposal', link: '/reference/spec/catalog/' },
              ],
            },
            pydocsSidebarGroup,
            { label: 'Rust API', link: 'https://docs.rs/zstar/latest/zstar/' },
            { label: 'Go API', link: 'https://pkg.go.dev/github.com/brian14708/zstar/go' },
          ],
        },
      ],
    }),
  ],
});
