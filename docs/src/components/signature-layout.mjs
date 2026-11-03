import { parseFragment, serialize } from 'parse5';

// Change text nodes only, so the plugin's syntax colors and type links survive.
export function formatSignatureHtml(html) {
  const fragment = parseFragment(html);
  visit(fragment, (element) => {
    if (element.tagName !== 'pre') return;
    const nodes = [];
    visit(element, (node) => {
      if (node.nodeName === '#text') nodes.push(node);
    });
    const text = nodes.map((node) => node.value).join('');
    if (text.length <= 88) return;
    const start = text.match(/^(?:@overload\s+)?(?:async )?(?:def|class) \w+\(/);
    if (!start) return;

    const characters = text.split('');
    let depth = 1;
    let quote;
    let escaped = false;
    let skipSpace = false;
    const opening = start[0].length - 1;
    if (text[opening + 1] === ')') return;
    characters[opening] += '\n    ';
    for (let index = opening + 1; index < text.length; index++) {
      const char = text[index];
      if (quote) {
        if (escaped) escaped = false;
        else if (char === '\\') escaped = true;
        else if (char === quote) quote = undefined;
        continue;
      }
      if (skipSpace && char === ' ') {
        characters[index] = '';
        continue;
      }
      skipSpace = false;
      if (char === '"' || char === "'") quote = char;
      else if ('([{'.includes(char)) depth++;
      else if (')]}'.includes(char)) {
        depth--;
        if (depth === 0) {
          characters[index] = '\n' + char;
          break;
        }
      } else if (char === ',' && depth === 1) {
        characters[index] += '\n    ';
        skipSpace = true;
      }
    }

    let offset = 0;
    for (const node of nodes) {
      const end = offset + node.value.length;
      node.value = characters.slice(offset, end).join('');
      offset = end;
    }
    element.attrs.push({ name: 'data-signature-layout', value: 'vertical' });
  });
  return serialize(fragment);
}

function visit(node, callback) {
  callback(node);
  for (const child of node.childNodes ?? []) visit(child, callback);
}
