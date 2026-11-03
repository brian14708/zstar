import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const docsRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repositoryRoot = path.resolve(docsRoot, '..');
const contentRoot = path.join(docsRoot, 'src/content/docs');
const pages = [
  ['spec/v1.md', 'reference/spec', 'Zstar v1 format specification'],
  ['python/README.md', 'sdks/python', 'Python SDK'],
  ['crates/zstar/README.md', 'sdks/rust', 'Rust SDK'],
  ['go/README.md', 'sdks/go', 'Go SDK'],
];

for (const [source, route, title] of pages) {
  const destination = path.join(docsRoot, `src/content/docs/${route}.md`);
  let content = await readFile(path.join(repositoryRoot, source), 'utf8');
  // Keep the published title in frontmatter while preserving existing anchors.
  content = content.replace(/^# .+\r?\n/, '');
  content = content.replaceAll('(../spec/v1.md)', '(/zstar/reference/spec/)');
  content = content.replaceAll(
    'https://github.com/brian14708/zstar/blob/main/spec/v1.md',
    '/zstar/reference/spec/',
  );
  // README links point to repository sources; published links point to routes.
  content = content.replace(/\]\(([^)]+)\)/g, (match, target) => {
    if (/^(?:[a-z]+:|\/|#)/i.test(target)) return match;
    const [file, fragment] = target.split('#');
    const resolved = path.resolve(path.dirname(path.join(repositoryRoot, source)), file);
    const relative = path.relative(contentRoot, resolved);
    if (relative.startsWith('..') || !/\.mdx?$/.test(relative)) return match;
    const route = relative.replace(/\.mdx?$/, '').split(path.sep).join('/');
    return `](/zstar/${route}/${fragment ? `#${fragment}` : ''})`;
  });
  if (route === 'sdks/python') {
    content = `[Python API reference](/zstar/api/python/) · [Source](https://github.com/brian14708/zstar/tree/main/python)\n\n${content}`;
  }
  await mkdir(path.dirname(destination), { recursive: true });
  await writeFile(destination, `---\ntitle: ${title}\n---\n\n${content}`);
}
