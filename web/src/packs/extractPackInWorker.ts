import type { PackSource } from './extractPack';
import type { ExtractRequest, ExtractResponse } from './extractPack.worker';
import { DownloadOptions, ExtractedPack } from './types';

// Runs extractPack in a worker, which is the only place browsers let it write
// files with sync access handles, and which keeps unzipping a large pack from
// freezing the page
export function extractPackInWorker(
  source: PackSource,
  filesDir: string,
  options: DownloadOptions = {},
): Promise<ExtractedPack> {
  const { onProgress, signal } = options;
  return new Promise((resolve, reject) => {
    const worker = new Worker(
      new URL('./extractPack.worker.ts', import.meta.url),
      { type: 'module' },
    );
    const request = (message: ExtractRequest) => worker.postMessage(message);
    // The worker removes what it unzipped before reporting that it stopped
    const onAbort = () => request({ type: 'abort' });
    signal?.addEventListener('abort', onAbort);
    const finish = () => {
      signal?.removeEventListener('abort', onAbort);
      worker.terminate();
    };
    worker.onmessage = (event: MessageEvent<ExtractResponse>) => {
      const response = event.data;
      if (response.type === 'progress') {
        if (typeof onProgress === 'function') onProgress(response.value);
        return;
      }
      finish();
      if (response.type === 'done') {
        resolve(response.result);
        return;
      }
      const error = new Error(response.message);
      error.name = response.name;
      reject(error);
    };
    worker.onerror = event => {
      finish();
      reject(new Error(`Could not unzip a pack into ${filesDir}: ${event.message}`));
    };
    request({ type: 'extract', filesDir, source });
    if (signal?.aborted) onAbort();
  });
}
