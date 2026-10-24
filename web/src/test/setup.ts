import 'fake-indexeddb/auto';
import { Blob as NodeBlob, File as NodeFile } from 'node:buffer';
import * as workerThreads from 'node:worker_threads';
import { cleanup } from '@testing-library/react';
// Importing opfs-mock also installs an in-memory origin private file system at
// navigator.storage, where packs keep their unzipped files
import { resetMockOPFS } from 'opfs-mock';
import { afterEach, beforeEach } from 'vitest';

// Testing Library only unmounts between tests on its own when Vitest globals are enabled
afterEach(cleanup);

beforeEach(() => resetMockOPFS());

// jsdom has no Web Locks, which keep tabs from removing pack files another tab is using, but
// Node has its own. The @types/node version this uses doesn't know about them yet.
const { locks } = workerThreads as unknown as { locks: LockManager };
Object.defineProperty(navigator, 'locks', { configurable: true, value: locks });

// fake-indexeddb copies stored values with Node's structuredClone, which turns jsdom's Blob
// into an empty object, so packs an older version of the app stored with their files in
// IndexedDB would have lost them. Node's own Blob survives the copy.
globalThis.Blob = NodeBlob as unknown as typeof Blob;
globalThis.File = NodeFile as unknown as typeof File;
