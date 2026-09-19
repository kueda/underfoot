import { act, renderHook, waitFor } from '@testing-library/react';
import localforage from 'localforage';
import {
  afterEach, beforeEach, describe, expect, it, vi,
} from 'vitest';
import {
  DOWNLOAD_CHUNKS, packMetadata, packZip, publishPack, StaticServer, stubStaticServer,
} from '../test/packFixtures';
import { Pack } from './Pack';
import { usePackStore } from './usePackStore';

const OAKLAND = 'us-ca-oakland';
const OAKLAND_URL = 'https://static.underfoot.rocks/us-ca-oakland.pmtiles.zip';

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

function rocksText(pack?: Pack) {
  return pack?.data?.rocks_pmtiles?.text();
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
      expect(fetchMock).toHaveBeenLastCalledWith(OAKLAND_URL, expect.any(Object));
      const pack = await result.current.get(OAKLAND);
      expect(pack).toBeInstanceOf(Pack);
      expect(await rocksText(pack)).toBe('oakland v1');
      expect(pack?.downloadedAt).toBe('2026-01-15T00:00:00.000Z');
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
      expect(fetchMock).toHaveBeenLastCalledWith(OAKLAND_URL, expect.any(Object));
      expect(await rocksText(await result.current.get(OAKLAND))).toBe('oakland v2');
    });
  });

  describe('get', () => {
    it('fills in pmtilesPath from the manifest for packs stored without it', async () => {
      // Older versions of the app stored downloaded packs without their pmtiles_path
      const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'oakland v1' });
      const storedPack = await Pack.fromZip(packMetadata({ pmtiles_path: undefined }), zip);
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

    it('discards a pack stored in an old format without unzipped data', async () => {
      await packStorage.setItem(OAKLAND, new Pack(packMetadata()));
      const { result } = await renderPackStore();
      const pack = await result.current.get(OAKLAND);
      expect(pack?.id).toBe(OAKLAND);
      expect(pack?.data).toBeUndefined();
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
      expect(packs[1].data).toBeUndefined();
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
      expect((await result.current.get(OAKLAND))?.data).toBeUndefined();
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
  });
});
