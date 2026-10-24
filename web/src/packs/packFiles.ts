import { UnzippedPackData } from './types';

// Packs keep their unzipped files in the origin private file system, under
// packs/<pack id>/<version>/, so a large pack's files never have to fit in
// memory, which storing them in IndexedDB as Blobs needs while they're
// written. Each file is named after its key in UnzippedPackData.
const PACKS_DIR = 'packs';

// The files in a pack zip the app uses, and their keys in UnzippedPackData
export const FILE_KEYS_BY_NAME: Record<string, keyof UnzippedPackData> = {
  'context.pmtiles': 'context_pmtiles',
  'contours.pmtiles': 'contours_pmtiles',
  'rocks-citations.csv': 'rocks_citations_csv',
  'rocks-rock_units_attrs.csv': 'rocks_units_csv',
  'rocks.pmtiles': 'rocks_pmtiles',
  'water-citations.csv': 'water_citations_csv',
  'water.pmtiles': 'water_pmtiles',
  'ways.pmtiles': 'ways_pmtiles',
};

const PACK_FILE_KEYS = Object.values(FILE_KEYS_BY_NAME);

// Other tabs of the app can't see what this one is doing with a folder of
// pack files, so a tab holds a lock named after the folder while it writes
// the folder's files or its map shows them, and only removes a folder whose
// lock it can take
function lockName(filesDir: string) {
  return `underfoot-pack-files:${filesDir}`;
}

// Holds the lock on a folder while fn runs, like while unzipping into it and
// then storing the pack that uses it
export function withPackFilesLock<T>(filesDir: string, fn: () => Promise<T>): Promise<T> {
  return navigator.locks.request(lockName(filesDir), fn) as Promise<T>;
}

// Shares the lock on a folder with other readers until the returned function
// is called, like while the map shows the pack whose files these are
export async function holdPackFiles(filesDir: string): Promise<() => void> {
  let release = () => {};
  const released = new Promise<void>(resolve => {
    release = resolve;
  });
  await new Promise<void>((granted, failed) => {
    navigator.locks.request(lockName(filesDir), { mode: 'shared' }, () => {
      granted();
      return released;
    }).catch(failed);
  });
  return release;
}

function isNotFound(error: unknown) {
  return error instanceof DOMException && error.name === 'NotFoundError';
}

// Pack ids can have characters a file name can't, like the colon in a local
// pack's id
function packDirName(packId: string) {
  return encodeURIComponent(packId);
}

// Where a new version of a pack's files goes, relative to the packs folder
export function newPackFilesDir(packId: string) {
  return `${packDirName(packId)}/${crypto.randomUUID()}`;
}

async function packsDir() {
  const root = await navigator.storage.getDirectory();
  return root.getDirectoryHandle(PACKS_DIR, { create: true });
}

export async function getPackFilesDir(filesDir: string, options?: { create: boolean }) {
  let dir = await packsDir();
  for (const name of filesDir.split('/')) {
    dir = await dir.getDirectoryHandle(name, options);
  }
  return dir;
}

export async function readPackFiles(filesDir: string): Promise<UnzippedPackData> {
  const dir = await getPackFilesDir(filesDir);
  const data: UnzippedPackData = {};
  for await (const [name, handle] of dir.entries()) {
    const key = name as keyof UnzippedPackData;
    if (handle.kind === 'file' && PACK_FILE_KEYS.includes(key)) {
      data[key] = await (handle as FileSystemFileHandle).getFile();
    }
  }
  return data;
}

// Removes a folder of a pack's files, like one it was partway through
// unzipping into when that failed
export async function removePackFilesDir(filesDir: string) {
  const names = filesDir.split('/');
  const last = names.pop() as string;
  try {
    const parent = names.length > 0 ? await getPackFilesDir(names.join('/')) : await packsDir();
    await parent.removeEntry(last, { recursive: true });
  }
  catch (error) {
    if (!isNotFound(error)) throw error;
  }
}

// Removes every version of a pack's files
export async function removePackFiles(packId: string) {
  await removePackFilesDir(packDirName(packId));
}

// Removes every version of every pack's files that isn't one of these and
// that no tab holds the lock on
export async function removeUnusedPackFiles(filesDirsInUse: string[]) {
  const dir = await packsDir();
  const packDirs: [string, FileSystemDirectoryHandle][] = [];
  for await (const [name, handle] of dir.entries()) {
    if (handle.kind === 'directory') packDirs.push([name, handle as FileSystemDirectoryHandle]);
  }
  await Promise.all(packDirs.map(async ([packDir, handle]) => {
    const versions: string[] = [];
    for await (const version of handle.keys()) versions.push(version);
    const unused = versions.filter(version => !filesDirsInUse.includes(`${packDir}/${version}`));
    await Promise.all(unused.map(version => navigator.locks.request(
      lockName(`${packDir}/${version}`),
      { ifAvailable: true },
      async lock => {
        if (lock) await handle.removeEntry(version, { recursive: true });
      },
    )));
    // Not recursively, since another tab may have started unzipping a new
    // version into it since its versions were listed
    try {
      await dir.removeEntry(packDir);
    }
    catch (error) {
      if (!(error instanceof DOMException && error.name === 'InvalidModificationError')) {
        throw error;
      }
    }
  }));
}
