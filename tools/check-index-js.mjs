import { readFileSync } from 'node:fs';

const file = process.argv[2] || 'index.html';
const html = readFileSync(file, 'utf8');
const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)].map((m) => m[1]);

if (!scripts.length) {
  throw new Error(`No inline scripts found in ${file}`);
}

for (const [index, script] of scripts.entries()) {
  try {
    // Syntax-only check. Browser globals are not executed.
    new Function(script);
  } catch (error) {
    error.message = `${file} inline script #${index + 1}: ${error.message}`;
    throw error;
  }
}

console.log(`Checked ${scripts.length} inline script block(s) in ${file}`);
