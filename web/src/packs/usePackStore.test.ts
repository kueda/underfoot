import { act, renderHook, waitFor } from '@testing-library/react';
import localforage from 'localforage';
import {
  afterEach, beforeEach, describe, expect, it, vi,
} from 'vitest';
import {
  DOWNLOAD_CHUNKS, packMetadata, packZip, publishPack, StaticServer, stubStaticServer,
} from '../test/packFixtures';
import { extractPack } from './extractPack';
import { extractPackInWorker } from './extractPackInWorker';
import { Pack } from './Pack';
import { holdPackFiles, newPackFilesDir } from './packFiles';
import { usePackStore } from './usePackStore';

// jsdom has no Worker, so unzip packs on the main thread instead
vi.mock('./extractPackInWorker', async () => {
  const { extractPack: extract } = await import('./extractPack');
  return { extractPackInWorker: vi.fn(extract) };
});

const OAKLAND = 'us-ca-oakland';

// Like a browser that hasn't decided whether to persist storage, as when Firefox is still
// asking the user
function neverDecideToPersist() {
  vi.spyOn(navigator.storage, 'persist').mockReturnValue(new Promise(() => {}));
}

// Makes storing a pack in IndexedDB fail the way it does when the storage quota runs out
function exceedQuotaWhenStoring() {
  vi.spyOn(IDBObjectStore.prototype, 'put').mockImplementation(() => {
    throw new DOMException('The quota has been exceeded.', 'QuotaExceededError');
  });
}
// Each version of a pack is requested at its own URL, so neither the CDN nor the browser can
// answer with a cached copy of an older version
const OAKLAND_V1_URL = 'https://static.underfoot.rocks/us-ca-oakland.pmtiles.zip'
  + '?v=2026-01-01T00%3A00%3A00.000Z';
const OAKLAND_V2_URL = 'https://static.underfoot.rocks/us-ca-oakland.pmtiles.zip'
  + '?v=2026-02-01T00%3A00%3A00.000Z';

// The same IndexedDB stores usePackStore uses, for clearing between tests and for setting up
// packs stored by older versions of the app
const packStorage = localforage.createInstance({ name: 'packStore' });
const prefStorage = localforage.createInstance({ name: 'prefStore' });

async function renderPackStore() {
  const hook = renderHook(() => usePackStore());
  // Wait for the manifest request to succeed or fail, since most of the store depends on it
  await waitFor(() => {
    expect(hook.result.current.manifest || hook.result.current.error).toBeTruthy();
  });
  return hook;
}

function goOffline(server: StaticServer) {
  server.manifest = undefined;
  // usePackStore logs the failed manifest request
  vi.spyOn(console, 'error').mockImplementation(() => {});
}

async function rocksText(pack?: Pack) {
  return (await pack?.unzippedData())?.rocks_pmtiles?.text();
}

// Paths of all the files packs have unzipped into the origin private file system
async function packFilePaths() {
  const paths: string[] = [];
  async function walk(dir: FileSystemDirectoryHandle, prefix: string) {
    for await (const [name, handle] of dir.entries()) {
      if (handle.kind === 'directory') {
        await walk(handle as FileSystemDirectoryHandle, `${prefix}${name}/`);
      }
      else paths.push(`${prefix}${name}`);
    }
  }
  await walk(await navigator.storage.getDirectory(), '');
  return paths.sort();
}

// Makes the download from this URL stall after its first half until the returned function is
// called
function stallDownload(
  fetchMock: ReturnType<typeof stubStaticServer>['fetchMock'],
  server: StaticServer,
  url: string,
) {
  const serveEverything = fetchMock.getMockImplementation();
  if (!serveEverything) throw new Error('fetch is not stubbed');
  let finish = () => {};
  fetchMock.mockImplementation(async (requestedUrl: string) => {
    if (requestedUrl !== url) return serveEverything(requestedUrl);
    const path = new URL(url).pathname.slice(1);
    const bytes = new Uint8Array(await server.files[path].arrayBuffer());
    const half = Math.ceil(bytes.length / 2);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes.slice(0, half));
        finish = () => {
          controller.enqueue(bytes.slice(half));
          controller.close();
        };
      },
    });
    return new Response(body, { headers: { 'Content-Length': bytes.length.toString() } });
  });
  return () => finish();
}

describe('usePackStore', () => {
  let fetchMock: ReturnType<typeof stubStaticServer>['fetchMock'];
  let server: StaticServer;

  beforeEach(async () => {
    await Promise.all([packStorage.clear(), prefStorage.clear()]);
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime('2026-01-15T00:00:00.000Z');
    ({ fetchMock, server } = stubStaticServer());
    await publishPack(server, packMetadata(), 'oakland v1');
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('download', () => {
    it('stores the pack so get returns it with its data', async () => {
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      expect(fetchMock).toHaveBeenLastCalledWith(OAKLAND_V1_URL, expect.any(Object));
      const pack = await result.current.get(OAKLAND);
      expect(pack).toBeInstanceOf(Pack);
      expect(await rocksText(pack)).toBe('oakland v1');
      expect(pack?.downloadedAt).toBe('2026-01-15T00:00:00.000Z');
    });

    it('keeps the pack\'s files out of IndexedDB', async () => {
      // Phones can't fit the files of a large pack in memory, which storing them in IndexedDB
      // needs, so they only go in the origin private file system
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      const storedPack = await packStorage.getItem<Pack>(OAKLAND);
      expect(storedPack?.data).toBeUndefined();
      expect(await packFilePaths()).toEqual([
        expect.stringMatching(/^packs\/us-ca-oakland\/[^/]+\/rocks_pmtiles$/),
      ]);
    });

    it('requests persistent storage before downloading the pack', async () => {
      // Best-effort storage has a much lower quota, which large packs can exceed
      const persist = vi.spyOn(navigator.storage, 'persist');
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      expect(persist).toHaveBeenCalled();
      const packFetchOrder = fetchMock.mock.calls
        .findIndex(([url]) => url === OAKLAND_V1_URL);
      expect(persist.mock.invocationCallOrder[0])
        .toBeLessThan(fetchMock.mock.invocationCallOrder[packFetchOrder]);
    });

    it('downloads the pack when the browser will not persist storage', async () => {
      vi.spyOn(navigator.storage, 'persist').mockRejectedValue(new Error('denied'));
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v1');
    });

    it('downloads the pack without waiting for the browser to decide whether to persist storage',
      async () => {
        // The download would show no progress while Firefox asks the user
        neverDecideToPersist();
        const { result } = await renderPackStore();
        await act(() => result.current.download(OAKLAND));
        expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v1');
      });

    it('removes the unzipped files when the pack cannot be stored', async () => {
      // Left behind, they'd keep using the space a retry needs
      const { result } = await renderPackStore();
      exceedQuotaWhenStoring();
      await expect(result.current.download(OAKLAND))
        .rejects.toMatchObject({ name: 'QuotaExceededError' });
      expect(await packFilePaths()).toEqual([]);
    });

    it('reports how much has downloaded after each chunk', async () => {
      const { result } = await renderPackStore();
      const onProgress = vi.fn();
      await act(() => result.current.download(OAKLAND, { onProgress }));
      const totalBytes = server.files['us-ca-oakland.pmtiles.zip'].size;
      expect(onProgress).toHaveBeenCalledTimes(DOWNLOAD_CHUNKS);
      const progress = onProgress.mock.calls.map(([value]) => value as {
        loadedBytes: number; totalBytes: number;
      });
      expect(progress.every(value => value.totalBytes === totalBytes)).toBe(true);
      const loadedBytes = progress.map(value => value.loadedBytes);
      expect(loadedBytes).toEqual([...loadedBytes].sort((a, b) => a - b));
      expect(loadedBytes.at(-1)).toBe(totalBytes);
    });

    it('does not report progress when the download size is unknown', async () => {
      server.sendContentLength = false;
      const { result } = await renderPackStore();
      const onProgress = vi.fn();
      await act(() => result.current.download(OAKLAND, { onProgress }));
      expect(onProgress).not.toHaveBeenCalled();
      expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v1');
    });

    it('rejects a pack that is not in the manifest', async () => {
      const { result } = await renderPackStore();
      await expect(result.current.download('us-xx-nowhere')).rejects.toThrow();
    });

    it('makes the pack current when no pack is current', async () => {
      const { result } = await renderPackStore();
      expect(result.current.currentPackId).toBeNull();
      await act(() => result.current.download(OAKLAND));
      expect(result.current.currentPackId).toBe(OAKLAND);
      expect(await result.current.getCurrentPackId()).toBe(OAKLAND);
    });

    it('leaves the current pack alone when another pack is current', async () => {
      await publishPack(server, packMetadata({
        id: 'us-ca-berkeley',
        pmtiles_path: 'us-ca-berkeley.pmtiles.zip',
      }));
      const { result } = await renderPackStore();
      act(() => result.current.setCurrent('us-ca-berkeley'));
      await act(() => result.current.download(OAKLAND));
      expect(result.current.currentPackId).toBe('us-ca-berkeley');
    });

    describe('with the Umami tracker loaded', () => {
      let track: ReturnType<typeof vi.fn>;

      beforeEach(() => {
        track = vi.fn();
        vi.stubGlobal('umami', { track });
      });

      it('records a pack-download event for the pack', async () => {
        const { result } = await renderPackStore();
        await act(() => result.current.download(OAKLAND));
        expect(track).toHaveBeenCalledWith('pack-download', { pack: OAKLAND });
      });

      it('does not record an event when the download fails', async () => {
        const { result } = await renderPackStore();
        await expect(result.current.download('us-xx-nowhere')).rejects.toThrow();
        expect(track).not.toHaveBeenCalled();
      });
    });
  });

  describe('when the manifest has an update for a downloaded pack', () => {
    beforeEach(async () => {
      const { result, unmount } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      unmount();
      const update = packMetadata({ updated_at: '2026-02-01T00:00:00.000Z' });
      await publishPack(server, update, 'oakland v2');
    });

    it('reports the manifest version alongside when the pack was downloaded', async () => {
      const { result } = await renderPackStore();
      const pack = await result.current.get(OAKLAND);
      expect(pack?.updatedAt).toBe('2026-02-01T00:00:00.000Z');
      expect(pack?.downloadedAt).toBe('2026-01-15T00:00:00.000Z');
      expect(await rocksText(pack)).toBe('oakland v1');
    });

    it('downloads the update from the manifest pmtiles_path', async () => {
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      expect(fetchMock).toHaveBeenLastCalledWith(OAKLAND_V2_URL, expect.any(Object));
      expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v2');
    });

    it('keeps the version it has when downloading the update is aborted', async () => {
      const { result } = await renderPackStore();
      const ac = new AbortController();
      const download = result.current.download(OAKLAND, {
        onProgress: () => ac.abort(),
        signal: ac.signal,
      });
      await expect(download).rejects.toMatchObject({ name: 'AbortError' });
      expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v1');
      expect(await packFilePaths()).toHaveLength(1);
    });

    it('keeps the files of the version it replaced until the app next loads', async () => {
      const { result, unmount } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      // The map may still be showing the old version, which it reads from its files
      expect(await packFilePaths()).toHaveLength(2);
      unmount();

      vi.resetModules();
      const { usePackStore: reloadedUsePackStore } = await import('./usePackStore');
      const reloaded = renderHook(() => reloadedUsePackStore());
      await waitFor(async () => expect(await packFilePaths()).toHaveLength(1));
      expect(await rocksText(await reloaded.result.current.get(OAKLAND))).toBe('oakland v2');
    });

    describe('when the app loads in another tab', () => {
      // Loads the store again, like another tab would, and waits for it to remove pack files
      // nothing uses, which an orphaned folder shows it has done
      async function loadInAnotherTab() {
        await extractPack(
          { file: await packZip({ 'orphan/rocks.pmtiles': 'orphan' }) },
          newPackFilesDir('us-ca-orphan'),
        );
        vi.resetModules();
        const { usePackStore: otherTabUsePackStore } = await import('./usePackStore');
        renderHook(() => otherTabUsePackStore());
        await waitFor(async () => {
          expect((await packFilePaths()).some(path => path.includes('orphan'))).toBe(false);
        });
      }

      it('keeps the files of the version this tab\'s map is still showing', async () => {
        const { result } = await renderPackStore();
        const v1FilesDir = (await packStorage.getItem<Pack>(OAKLAND))?.filesDir as string;
        const releaseV1 = await holdPackFiles(v1FilesDir);
        await act(() => result.current.download(OAKLAND));
        await loadInAnotherTab();
        expect(await packFilePaths()).toHaveLength(2);
        releaseV1();
      });

      it('keeps the files of an update this tab is still downloading', async () => {
        const finishDownload = stallDownload(fetchMock, server, OAKLAND_V2_URL);
        const onProgress = vi.fn();
        const { result } = await renderPackStore();
        const download = result.current.download(OAKLAND, { onProgress });
        await waitFor(() => expect(onProgress).toHaveBeenCalled());
        await loadInAnotherTab();
        finishDownload();
        await act(() => download);
        expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v2');
      });
    });
  });

  describe('get', () => {
    it('fills in pmtilesPath from the manifest for packs stored without it', async () => {
      // Older versions of the app stored downloaded packs without their pmtiles_path
      const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'oakland v1' });
      const storedPack = Pack.fromExtracted(
        packMetadata({ pmtiles_path: undefined }),
        await extractPack({ file: zip }, newPackFilesDir(OAKLAND)),
      );
      await packStorage.setItem(OAKLAND, storedPack);
      const { result } = await renderPackStore();
      const pack = await result.current.get(OAKLAND);
      expect(pack?.pmtilesPath).toBe('us-ca-oakland.pmtiles.zip');
      expect(await rocksText(pack)).toBe('oakland v1');
    });

    it('keeps pmtilesPath for a downloaded pack when the manifest is unavailable', async () => {
      const online = await renderPackStore();
      await act(() => online.result.current.download(OAKLAND));
      online.unmount();
      goOffline(server);
      const { result } = await renderPackStore();
      expect(result.current.manifest).toBeUndefined();
      expect((await result.current.get(OAKLAND))?.pmtilesPath).toBe('us-ca-oakland.pmtiles.zip');
    });

    it('reads a pack an older version of the app stored with its files in IndexedDB', async () => {
      await packStorage.setItem(OAKLAND, new Pack(packMetadata(), {
        rocks_pmtiles: new Blob(['oakland v1']),
      }));
      const { result } = await renderPackStore();
      expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v1');
    });

    it('discards a pack stored in an old format without unzipped data', async () => {
      await packStorage.setItem(OAKLAND, new Pack(packMetadata()));
      const { result } = await renderPackStore();
      const pack = await result.current.get(OAKLAND);
      expect(pack?.id).toBe(OAKLAND);
      expect(pack?.isDownloaded()).toBe(false);
      expect(await packStorage.getItem(OAKLAND)).toBeNull();
    });
  });

  describe('list', () => {
    it('lists manifest packs that have pmtiles, using downloaded copies', async () => {
      await publishPack(server, packMetadata({
        id: 'us-ca-berkeley',
        pmtiles_path: 'us-ca-berkeley.pmtiles.zip',
      }));
      await publishPack(server, packMetadata({ id: 'us-ca-unbuilt', pmtiles_path: undefined }));
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      const packs = await result.current.list();
      expect(packs.map(pack => pack.id)).toEqual([OAKLAND, 'us-ca-berkeley']);
      expect(await rocksText(packs[0])).toBe('oakland v1');
      expect(packs[1].isDownloaded()).toBe(false);
    });

    it('lists downloaded packs when the manifest is unavailable', async () => {
      const online = await renderPackStore();
      await act(() => online.result.current.download(OAKLAND));
      online.unmount();
      goOffline(server);
      const { result } = await renderPackStore();
      const packs = await result.current.list();
      expect(packs.map(pack => pack.id)).toEqual([OAKLAND]);
      expect(await rocksText(packs[0])).toBe('oakland v1');
    });
  });

  describe('remove', () => {
    it('deletes the stored pack and stops it being the current pack', async () => {
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      await act(() => result.current.remove(OAKLAND));
      expect((await result.current.get(OAKLAND))?.isDownloaded()).toBe(false);
      expect(await packFilePaths()).toEqual([]);
      expect(result.current.currentPackId).toBeNull();
      await waitFor(async () => expect(await result.current.getCurrentPackId()).toBeNull());
    });

    it('leaves the current pack alone when removing another pack', async () => {
      await publishPack(server, packMetadata({
        id: 'us-ca-berkeley',
        pmtiles_path: 'us-ca-berkeley.pmtiles.zip',
      }));
      const { result } = await renderPackStore();
      await act(() => result.current.download(OAKLAND));
      await act(() => result.current.download('us-ca-berkeley'));
      await act(() => result.current.remove('us-ca-berkeley'));
      expect(result.current.currentPackId).toBe(OAKLAND);
    });
  });

  describe('setCurrent', () => {
    it('remembers the current pack the next time the store loads', async () => {
      const first = await renderPackStore();
      act(() => first.result.current.setCurrent(OAKLAND));
      await waitFor(async () => {
        expect(await first.result.current.getCurrentPackId()).toBe(OAKLAND);
      });
      first.unmount();
      const { result } = await renderPackStore();
      await waitFor(() => expect(result.current.currentPackId).toBe(OAKLAND));
    });
  });

  describe('addFromFile', () => {
    it('stores a pack from a local file under a local id', async () => {
      const zip = await packZip({ 'Oakland Hills/rocks.pmtiles': 'local rocks' });
      const file = new File([zip], 'Oakland Hills.pmtiles.zip');
      const { result } = await renderPackStore();
      const packId = await act(() => result.current.addFromFile(file));
      expect(packId).toBe('local:oakland-hills');
      expect(await rocksText(await result.current.get(packId))).toBe('local rocks');
    });

    it('titles the stored pack from the pack.json in the zip', async () => {
      const zip = await packZip({
        'us-ca-oakland.pmtiles/pack.json': JSON.stringify(packMetadata({
          description: 'Oakland, CA, USA. Mostly for testing some place small.',
          name: 'Oakland, CA, USA',
        })),
        'us-ca-oakland.pmtiles/rocks.pmtiles': 'local rocks',
      });
      const file = new File([zip], 'us-ca-oakland.pmtiles.zip');
      const { result } = await renderPackStore();
      const packId = await act(() => result.current.addFromFile(file));
      expect(packId).toBe('local:us-ca-oakland');
      expect(await result.current.get(packId)).toMatchObject({
        description: 'Oakland, CA, USA. Mostly for testing some place small.',
        name: 'Oakland, CA, USA',
        sourceFileName: 'us-ca-oakland.pmtiles.zip',
      });
    });

    it('requests persistent storage before unzipping the pack', async () => {
      // Best-effort storage has a much lower quota, which large packs can exceed
      const persist = vi.spyOn(navigator.storage, 'persist');
      const zip = await packZip({ 'Oakland Hills/rocks.pmtiles': 'local rocks' });
      const file = new File([zip], 'Oakland Hills.pmtiles.zip');
      const { result } = await renderPackStore();
      await act(() => result.current.addFromFile(file));
      expect(persist.mock.invocationCallOrder[0])
        .toBeLessThan(vi.mocked(extractPackInWorker).mock.invocationCallOrder.at(-1) as number);
    });

    it('stores the pack without waiting for the browser to decide whether to persist storage',
      async () => {
        neverDecideToPersist();
        const zip = await packZip({ 'Oakland Hills/rocks.pmtiles': 'local rocks' });
        const file = new File([zip], 'Oakland Hills.pmtiles.zip');
        const { result } = await renderPackStore();
        const packId = await act(() => result.current.addFromFile(file));
        expect(await rocksText(await result.current.get(packId))).toBe('local rocks');
      });

    it('removes the unzipped files when the pack cannot be stored', async () => {
      const zip = await packZip({ 'Oakland Hills/rocks.pmtiles': 'local rocks' });
      const file = new File([zip], 'Oakland Hills.pmtiles.zip');
      const { result } = await renderPackStore();
      exceedQuotaWhenStoring();
      await expect(result.current.addFromFile(file))
        .rejects.toMatchObject({ name: 'QuotaExceededError' });
      expect(await packFilePaths()).toEqual([]);
    });

    it('stores the pack when the browser will not persist storage', async () => {
      vi.spyOn(navigator.storage, 'persist').mockRejectedValue(new Error('denied'));
      const zip = await packZip({ 'Oakland Hills/rocks.pmtiles': 'local rocks' });
      const file = new File([zip], 'Oakland Hills.pmtiles.zip');
      const { result } = await renderPackStore();
      const packId = await act(() => result.current.addFromFile(file));
      expect(await rocksText(await result.current.get(packId))).toBe('local rocks');
    });
  });
});
