import { Unzip, UnzipInflate } from 'fflate';
import { FILE_KEYS_BY_NAME, getPackFilesDir, removePackFilesDir } from './packFiles';
import { DownloadOptions, ExtractedPack, UnzippedPackData } from './types';

// A zip ends with an end of central directory record, which says how many
// files the zip has. The record is 22 bytes plus a comment of up to 65,535
// bytes, and zips too big for it put a 56-byte zip64 record and a 20-byte
// locator before it.
const EOCD_SIGNATURE = 0x06054b50;
const ZIP64_EOCD_SIGNATURE = 0x06064b50;
const ZIP_TAIL_BYTES = 22 + 65535 + 20 + 56;

// How many files the zip whose last bytes these are says it has, if those
// bytes include its end of central directory record
function zipEntryCount(tail: Uint8Array) {
  const view = new DataView(tail.buffer, tail.byteOffset, tail.byteLength);
  for (let i = tail.length - 22; i >= 0; i -= 1) {
    if (view.getUint32(i, true) === EOCD_SIGNATURE) {
      const count = view.getUint16(i + 10, true);
      if (count !== 0xffff) return count;
      const zip64Start = i - 20 - 56;
      if (zip64Start >= 0 && view.getUint32(zip64Start, true) === ZIP64_EOCD_SIGNATURE) {
        return Number(view.getBigUint64(zip64Start + 32, true));
      }
      return undefined;
    }
  }
  return undefined;
}

export type PackSource = { file: Blob } | { url: string };

// A file in the zip being written to the pack's folder. Unzip hands over each
// chunk of a file as soon as the zip data is pushed into it, but opening a
// file to write to has to wait for a promise, so chunks wait here until
// they're written.
interface PendingFile {
  chunks: Uint8Array[];
  done: boolean;
  error?: Error;
  handle?: FileSystemSyncAccessHandle;
  key: keyof UnzippedPackData;
  written: number;
}

async function openSource(source: PackSource, signal?: AbortSignal) {
  if ('file' in source) {
    return { body: source.file.stream(), totalBytes: source.file.size };
  }
  const resp = await fetch(source.url, { signal });
  if (!resp.ok || !resp.body) {
    throw new Error(`Could not download ${source.url}: ${resp.status} ${resp.statusText}`);
  }
  const contentLength = resp.headers.get('Content-Length');
  return { body: resp.body, totalBytes: contentLength ? parseInt(contentLength, 10) : null };
}

// Unzips a pack zip into a new folder in the origin private file system (see
// newPackFilesDir) as it downloads or reads from disk, so no more than a chunk
// of it is ever in memory. That's what lets phones open packs that are more
// than a gigabyte. This writes with sync access handles, which browsers only
// allow in a worker, so the app runs it with extractPackInWorker.
export async function extractPack(
  source: PackSource,
  filesDir: string,
  options: DownloadOptions = {},
): Promise<ExtractedPack> {
  const { onProgress, signal } = options;
  const { body, totalBytes } = await openSource(source, signal);
  const dir = await getPackFilesDir(filesDir, { create: true });
  const reader = body.getReader();
  const pendingFiles: PendingFile[] = [];
  const packJsonChunks: Uint8Array[] = [];
  let entryCount = 0;
  let packFileCount = 0;
  let packJsonError: Error | undefined;
  let tail = new Uint8Array(0);

  // Keeps the last ZIP_TAIL_BYTES of the zip, which hold its end of central
  // directory record once it has all been read
  function keepTail(chunk: Uint8Array) {
    const length = Math.min(ZIP_TAIL_BYTES, tail.length + chunk.length);
    const newTail = new Uint8Array(length);
    const fromChunk = Math.min(chunk.length, length);
    newTail.set(tail.subarray(tail.length - (length - fromChunk)));
    newTail.set(chunk.subarray(chunk.length - fromChunk), length - fromChunk);
    tail = newTail;
  }

  const unzip = new Unzip(file => {
    entryCount += 1;
    const name = file.name.split('/').pop() || '';
    const key = FILE_KEYS_BY_NAME[name];
    if (key) {
      packFileCount += 1;
      const pending: PendingFile = {
        chunks: [], done: false, key, written: 0,
      };
      pendingFiles.push(pending);
      file.ondata = (error: Error | null, data: Uint8Array, final: boolean) => {
        if (error) pending.error = error;
        else pending.chunks.push(data);
        pending.done = final;
      };
    }
    else if (name === 'pack.json') {
      packFileCount += 1;
      file.ondata = (error: Error | null, data: Uint8Array) => {
        if (error) packJsonError = error;
        else packJsonChunks.push(data);
      };
    }
    else {
      // Unzip holds on to the data of a file that isn't started, so unzip
      // files the app doesn't use too, and let their data go
      file.ondata = () => {};
    }
    file.start();
  });
  unzip.register(UnzipInflate);

  async function writePendingFiles() {
    if (packJsonError) throw packJsonError;
    for (const pending of pendingFiles) {
      if (pending.error) throw pending.error;
      if (!pending.handle && (pending.chunks.length > 0 || pending.done)) {
        const fileHandle = await dir.getFileHandle(pending.key, { create: true });
        pending.handle = await fileHandle.createSyncAccessHandle();
        // A zip can have two files with the same name in different folders,
        // and the last one wins, like with other unzip tools
        pending.handle.truncate(0);
      }
      for (const chunk of pending.chunks) {
        pending.written += pending.handle?.write(chunk, { at: pending.written }) || 0;
      }
      pending.chunks = [];
      if (pending.done) {
        pending.handle?.flush();
        pending.handle?.close();
        pending.handle = undefined;
      }
    }
    // Leave just the files that are still being unzipped
    pendingFiles.splice(0, pendingFiles.length, ...pendingFiles.filter(p => !p.done));
  }

  try {
    // Start chunked download with progress, based on https://javascript.info/fetch-progress
    let loadedBytes = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      signal?.throwIfAborted();
      const { done, value } = await reader.read();
      if (done) break;
      keepTail(value);
      unzip.push(value);
      await writePendingFiles();
      loadedBytes += value.length;
      if (totalBytes && typeof onProgress === 'function') {
        onProgress({ loadedBytes, totalBytes });
      }
    }
    // Throws if the zip was cut off partway through a file
    unzip.push(new Uint8Array(0), true);
    await writePendingFiles();
    // Unzip reads files as it comes to them, so it can't tell when a zip
    // ends between two files
    const expectedEntryCount = zipEntryCount(tail);
    if (expectedEntryCount === undefined || expectedEntryCount !== entryCount) {
      throw new Error(
        `The pack zip is cut off: it has ${entryCount} files, `
        + `but should have ${expectedEntryCount ?? 'more'}`,
      );
    }
    if (packFileCount === 0) throw new Error('No pack files in the zip');
  }
  catch (error) {
    // Stop downloading, if it's still going
    await reader.cancel().catch(() => {});
    // A file can't be removed while it's open
    pendingFiles.forEach(pending => pending.handle?.close());
    // Report why unzipping stopped, like an abort, even if removing what it
    // unzipped fails too
    await removePackFilesDir(filesDir).catch((removeError: unknown) => {
      console.error('Failed to remove the files of a failed unzip in %s:', filesDir, removeError);
    });
    throw error;
  }
  const packJson = packJsonChunks.length > 0
    ? await new Blob(packJsonChunks).text()
    : undefined;
  return { filesDir, packJson };
}
