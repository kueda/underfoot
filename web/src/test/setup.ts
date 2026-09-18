import 'fake-indexeddb/auto';
import { Blob as NodeBlob, File as NodeFile } from 'node:buffer';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

// Testing Library only unmounts between tests on its own when Vitest globals are enabled
afterEach(cleanup);

// fake-indexeddb copies stored values with Node's structuredClone, which turns jsdom's Blob
// into an empty object, so packs read back from IndexedDB would have lost their data. Node's
// own Blob survives the copy, but jsdom's FileReader (which jszip uses to read a Blob) won't
// read it, so this also swaps in a FileReader that only does what jszip needs.
class NodeBlobFileReader {
  error: unknown = null;
  onerror: ((event: { target: NodeBlobFileReader }) => void) | null = null;
  onload: ((event: { target: NodeBlobFileReader }) => void) | null = null;
  result: ArrayBuffer | null = null;

  readAsArrayBuffer(blob: Blob) {
    blob.arrayBuffer().then(
      buffer => {
        // Node's Blob returns Node's ArrayBuffer, but the jsdom environment replaces the
        // global ArrayBuffer with jsdom's, so jszip's `instanceof ArrayBuffer` check would
        // fail without copying the bytes into the global one.
        this.result = new ArrayBuffer(buffer.byteLength);
        new Uint8Array(this.result).set(new Uint8Array(buffer));
        this.onload?.({ target: this });
      },
      (error: unknown) => {
        this.error = error;
        this.onerror?.({ target: this });
      },
    );
  }
}

globalThis.Blob = NodeBlob as unknown as typeof Blob;
globalThis.File = NodeFile as unknown as typeof File;
globalThis.FileReader = NodeBlobFileReader as unknown as typeof FileReader;
