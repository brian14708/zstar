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
                'zstar.Archive', 'zstar.Archive.**',
                'zstar.AsyncArchive', 'zstar.AsyncArchive.**',
                'zstar.ArchiveWriter', 'zstar.ArchiveWriter.**',
                'zstar.AsyncArchiveWriter', 'zstar.AsyncArchiveWriter.**',
                'zstar.ZstarError', 'zstar.ZstarError.**',
                'zstar.types', 'zstar.types.**',
                'zstar.ZstarFileSystem', 'zstar.ZstarFileSystem.**',
              ],
            },
            sourceLink: { host: 'github', repo: 'brian14708/zstar', root: '..' },
          }],
        }),
      ],
      logo: { src: './public/logo.svg', alt: '' },
      description: 'Indexed tar.zst archives with random-access readers and writers.',
      customCss: ['./src/styles/typography.css', './src/styles/theme.css'],
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/brian14708/zstar' }],
      expressiveCode: { shiki: { langAlias: { cddl: 'text', abnf: 'text' } } },
      sidebar: [
        {
          label: 'Start here',
          items: [
            { label: 'Overview', link: '/' },
            { label: 'Getting started', link: '/getting-started/' },
          ],
        },
        {
          label: 'Python',
          items: [
            { label: 'Overview', link: '/sdks/python/' },
            { label: 'Create your first archive', link: '/tutorials/python/first-archive/' },
            { label: 'Read from S3', link: '/tutorials/python/s3/' },
            { label: 'Advanced usage', link: '/guides/python/configuration/' },
          ],
        },
        {
          label: 'Rust',
          items: [
            { label: 'Overview', link: '/sdks/rust/' },
            { label: 'Create your first archive', link: '/tutorials/rust/first-archive/' },
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
            { label: 'Format specification', link: '/reference/spec/' },
            pydocsSidebarGroup,
            { label: 'Rust API', link: 'https://docs.rs/zstar/latest/zstar/' },
            { label: 'Go API', link: 'https://pkg.go.dev/github.com/brian14708/zstar/go' },
          ],
        },
      ],
    }),
  ],
});
