import { useCallback, useEffect, useState } from 'react';
import localforage from 'localforage';
import { extractPackInWorker } from './extractPackInWorker';
import { Pack } from './Pack';
import {
  newPackFilesDir, removePackFiles, removePackFilesDir, removeUnusedPackFiles, withPackFilesLock,
} from './packFiles';

import { Manifest } from './Manifest';
import {
  DownloadOptions, PackStore, RemoteManifest,
} from './types';

const packStore = localforage.createInstance({ name: 'packStore' });
const prefStore = localforage.createInstance({ name: 'prefStore' });

// Some browsers, like Firefox, give best-effort storage a much lower quota
// than persistent storage, low enough that a large pack can exceed it. A pack
// is still stored if the browser won't persist storage, since it may fit
// anyway. Nothing waits for the answer, since Firefox asks the user, and the
// pack shouldn't sit there showing no progress until they reply.
function requestPersistentStorage() {
  navigator.storage.persist().catch(() => false);
}

// Stores a pack whose files were just unzipped into filesDir, removing them
// if that fails. Left behind, they'd keep using the space a retry needs until
// the app next loads, and failing to store a pack usually means it ran out.
async function storeOrRemoveFiles(filesDir: string, store: () => Promise<void>) {
  try {
    await store();
  }
  catch (error) {
    await removePackFilesDir(filesDir).catch((removeError: unknown) => {
      console.error(
        'Failed to remove the files of an unstored pack in %s:',
        filesDir,
        removeError,
      );
    });
    throw error;
  }
}

// Removes pack files no stored pack uses, like those of a version of a pack
// that was since updated, or of a download that was cut off when the app
// closed. This runs once per page load, rather than after each download, since
// the map may still be showing the version a download replaced. The locks
// packFiles.ts takes keep it from removing files that this or another tab is
// still writing or showing.
let unusedFilesRemoved: Promise<void> | undefined;
function removeUnusedFiles() {
  unusedFilesRemoved ||= (async () => {
    const packIds = await packStore.keys();
    const storedPacks = await Promise.all(packIds.map(id => packStore.getItem<Pack>(id)));
    await removeUnusedPackFiles(storedPacks.flatMap(pack => (
      pack?.filesDir ? [pack.filesDir] : []
    )));
  })().catch(e => console.error('Failed to remove unused pack files: ', e));
  return unusedFilesRemoved;
}

export function usePackStore(): PackStore {
  const [manifest, setManifest] = useState<Manifest | undefined>(undefined);
  const [currentPackId, setCurrentPackId] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    void removeUnusedFiles();
  }, []);

  useEffect(() => {
    prefStore.getItem<string>('currentPackId')
      .then(setCurrentPackId)
      .catch(e => console.error('Failed to get currentPackId: ', e));
  }, []);

  useEffect(() => {
    fetch('https://static.underfoot.rocks/manifest.json')
      .then(r => r.json())
      .then((json: RemoteManifest) => setManifest(new Manifest(json)))
      .catch((err: Error) => {
        console.error('[ERROR] oh no!: ', err);
        setError(err);
      });
  }, []);

  const getCurrentPackId = useCallback(async () => prefStore.getItem<string>('currentPackId'), []);

  const get = useCallback(async (packId: string): Promise<Pack | undefined> => {
    const storedPack = await packStore.getItem<Pack>(packId);
    if (storedPack?.filesDir || storedPack?.data) {
      // Instantiate a full Pack object so we have all the instance methods
      const localPack = Pack.fromPack(storedPack);
      // Use the manifest's current updatedAt so we can detect if a newer version is available.
      // Also refresh pmtilesPath from the manifest: older stored packs may have lost it (see
      // the pmtiles_path fix in download() below), which broke re-downloading updates.
      const manifestPack = manifest?.packs.find((pack: Pack) => pack.id === packId);
      if (manifestPack) {
        localPack.updatedAt = manifestPack.updatedAt;
        localPack.pmtilesPath = manifestPack.pmtilesPath;
      }
      return localPack;
    }
    if (storedPack) {
      // Pack was stored by an older version of the app in an incompatible
      // format; treat it as not downloaded rather than crashing on load.
      await packStore.removeItem(packId);
    }
    return manifest?.packs.find((pack: Pack) => pack.id === packId);
  }, [manifest]);

  const setCurrent = useCallback((packId: string | null) => {
    setCurrentPackId(packId);
    if (packId) {
      prefStore.setItem('currentPackId', packId)
        .catch(e => console.error('Failed to get currentPackId: ', e));
    }
    else {
      prefStore.removeItem('currentPackId')
        .catch(e => console.error('Failed to remove currentPackId: ', e));
    }
  }, []);

  const listLocal = useCallback(async () => {
    const localPackIds = await packStore.keys();
    const localPacks = await Promise.all(localPackIds.map(packId => get(packId)));
    return localPacks.flat() as Pack[];
  }, [get]);

  const list = useCallback(async () => {
    const manifestPacks = manifest?.packs;
    let packs: (Pack | undefined)[] = [];
    if (manifestPacks) {
      packs = await Promise.all(manifestPacks.filter(pack => pack.pmtilesPath).map(pack => get(pack.id)));
    }
    if (packs.length === 0) {
      packs = await listLocal();
    }
    return packs.flat() as Pack[];
  }, [get, listLocal, manifest]);

  // Saves a pack that was unzipped from an archive to packStore, whether the
  // archive came from a network download or a local file. Later pack loads
  // (including on a fresh page load) are then plain file lookups instead of
  // repeating the decompression work.
  const storePack = useCallback(async (storedPack: Pack) => {
    storedPack.downloadedAt = new Date().toISOString();
    await packStore.setItem(storedPack.id, storedPack);
    if (!currentPackId) {
      setCurrent(storedPack.id);
    }
  }, [currentPackId, setCurrent]);

  const download = useCallback(async (packId: string, options?: DownloadOptions) => {
    const opts = options || {};
    const pack = await get(packId);
    if (!pack) throw new Error('Can\'t unknown pack');
    requestPersistentStorage();
    const url = `https://static.underfoot.rocks/${pack.pmtilesPath}`;
    // Until the pack is stored, nothing says the files are in use but the lock
    const filesDir = newPackFilesDir(pack.id);
    await withPackFilesLock(filesDir, async () => {
      const extracted = await extractPackInWorker({ url }, filesDir, opts);
      await storeOrRemoveFiles(filesDir, () => storePack(Pack.fromExtracted({
        admin1: pack.admin1,
        admin2: pack.admin2,
        bbox: pack.bbox,
        description: pack.description,
        id: pack.id,
        name: pack.name,
        pmtiles_path: pack.pmtilesPath,
        updated_at: pack.updatedAt,
      }, extracted)));
    });
    window.umami?.track('pack-download', { pack: pack.id });
  }, [get, storePack]);

  // Loads a pack straight from a local .zip, bypassing the manifest/download
  // flow entirely, so a pack built with `packs.py` can be viewed without
  // hosting it anywhere first.
  const addFromFile = useCallback(async (file: File): Promise<string> => {
    requestPersistentStorage();
    const filesDir = newPackFilesDir(Pack.metadataFromFileName(file.name).id);
    return withPackFilesLock(filesDir, async () => {
      const localPack = Pack.fromLocalExtracted(
        file.name,
        await extractPackInWorker({ file }, filesDir),
      );
      await storeOrRemoveFiles(filesDir, () => storePack(localPack));
      return localPack.id;
    });
  }, [storePack]);

  const remove = useCallback(async (packId: string) => {
    await packStore.removeItem(packId);
    await removePackFiles(packId);
    if (currentPackId === packId) setCurrent(null);
  }, [currentPackId, setCurrent]);

  return {
    addFromFile,
    currentPackId,
    download,
    error,
    get,
    getCurrentPackId,
    list,
    listLocal,
    manifest,
    remove,
    setCurrent,
  };
}
