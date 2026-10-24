import { extractPack, PackSource } from './extractPack';
import { ExtractedPack } from './types';

export type ExtractRequest = { type: 'abort' }
  | { type: 'extract'; filesDir: string; source: PackSource };

export type ExtractResponse = { type: 'done'; result: ExtractedPack }
  | { type: 'error'; message: string; name: string }
  | { type: 'progress'; value: { loadedBytes: number; totalBytes: number } };

// TypeScript only knows the window's global scope here, not a worker's
const scope = self as unknown as Worker;
const abortController = new AbortController();

function respond(response: ExtractResponse) {
  scope.postMessage(response);
}

scope.onmessage = (event: MessageEvent<ExtractRequest>) => {
  const request = event.data;
  if (request.type === 'abort') {
    abortController.abort();
    return;
  }
  extractPack(request.source, request.filesDir, {
    onProgress: value => respond({ type: 'progress', value }),
    signal: abortController.signal,
  })
    .then(result => respond({ type: 'done', result }))
    .catch((error: Error) => respond({ type: 'error', message: error.message, name: error.name }));
};
