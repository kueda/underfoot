// Sync access handles for files in the origin private file system, which
// TypeScript only declares in its WebWorker lib since browsers only allow them
// in workers. extractPack.ts runs in one, but the app's tsconfig uses the DOM
// lib, which can't be combined with the WebWorker one.
interface FileSystemSyncAccessHandle {
  close(): void;
  flush(): void;
  getSize(): number;
  truncate(newSize: number): void;
  write(buffer: AllowSharedBufferSource, options?: { at?: number }): number;
}

interface FileSystemFileHandle {
  createSyncAccessHandle(): Promise<FileSystemSyncAccessHandle>;
}
