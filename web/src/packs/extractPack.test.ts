import { describe, expect, it, vi } from 'vitest';
import { packZip, stubStaticServer } from '../test/packFixtures';
import { extractPack, PackSource } from './extractPack';
import { newPackFilesDir, readPackFiles, removePackFilesDir } from './packFiles';
import { DownloadOptions } from './types';

// Lets a test make removing a failed pack's files fail too
vi.mock('./packFiles', async importOriginal => {
  const actual = await importOriginal<typeof import('./packFiles')>();
  return { ...actual, removePackFilesDir: vi.fn(actual.removePackFilesDir) };
});

const OAKLAND = 'us-ca-oakland';

function extractOakland(source: PackSource, options?: DownloadOptions) {
  return extractPack(source, newPackFilesDir(OAKLAND), options);
}

async function filesAsText(filesDir: string) {
  const files = await readPackFiles(filesDir);
  const entries = await Promise.all(
    Object.entries(files).map(async ([key, file]) => [key, await file.text()]),
  );
  return Object.fromEntries(entries) as Record<string, string>;
}

// Names of the folders a pack's versions are unzipped into
async function versionFolders(packId: string) {
  const root = await navigator.storage.getDirectory();
  let packDir;
  try {
    const packsDir = await root.getDirectoryHandle('packs');
    packDir = await packsDir.getDirectoryHandle(encodeURIComponent(packId));
  }
  catch {
    return [];
  }
  const names = [];
  for await (const name of packDir.keys()) names.push(name);
  return names;
}

// Where each file's local header starts in a zip, which is where the file starts
async function localHeaderOffsets(zip: Blob) {
  const bytes = new Uint8Array(await zip.arrayBuffer());
  const offsets = [];
  for (let i = 0; i + 4 <= bytes.length; i += 1) {
    if (bytes[i] === 0x50 && bytes[i + 1] === 0x4b && bytes[i + 2] === 3 && bytes[i + 3] === 4) {
      offsets.push(i);
    }
  }
  return offsets;
}

describe('extractPack', () => {
  it('unzips each known file from a folder in the zip and ignores other files', async () => {
    // jszip also writes an entry for the us-ca-oakland/ folder itself, like some zip tools do
    const zip = await packZip({
      'us-ca-oakland/context.pmtiles': 'context',
      'us-ca-oakland/contours.pmtiles': 'contours',
      'us-ca-oakland/README.txt': 'not part of a pack',
      'us-ca-oakland/rocks-citations.csv': 'rocks citations',
      'us-ca-oakland/rocks-rock_units_attrs.csv': 'rock units',
      'us-ca-oakland/rocks.pmtiles': 'rocks',
      'us-ca-oakland/water-citations.csv': 'water citations',
      // Packs built before waterways got flow labels in their tiles have this
      'us-ca-oakland/water-waterways_network.csv': 'waterways network',
      'us-ca-oakland/water.pmtiles': 'water',
      'us-ca-oakland/ways.pmtiles': 'ways',
    });
    const { filesDir } = await extractOakland({ file: zip });
    expect(await filesAsText(filesDir)).toEqual({
      context_pmtiles: 'context',
      contours_pmtiles: 'contours',
      rocks_citations_csv: 'rocks citations',
      rocks_pmtiles: 'rocks',
      rocks_units_csv: 'rock units',
      water_citations_csv: 'water citations',
      water_pmtiles: 'water',
      ways_pmtiles: 'ways',
    });
  });

  it('returns the text of the zip\'s pack.json', async () => {
    const zip = await packZip({
      'us-ca-oakland/pack.json': '{"name": "Oakland"}',
      'us-ca-oakland/rocks.pmtiles': 'rocks',
    });
    const { packJson } = await extractOakland({ file: zip });
    expect(packJson).toBe('{"name": "Oakland"}');
  });

  it('unzips a pack as it downloads, whatever chunks it arrives in', async () => {
    const { server } = stubStaticServer();
    // Long enough that each chunk ends partway through a file's data
    const rocks = 'rocks '.repeat(1000);
    server.files['us-ca-oakland.pmtiles.zip'] = await packZip({
      'us-ca-oakland/rocks.pmtiles': rocks,
      'us-ca-oakland/water.pmtiles': 'water',
    });
    const { filesDir } = await extractOakland({
      url: 'https://static.underfoot.rocks/us-ca-oakland.pmtiles.zip',
    });
    expect(await filesAsText(filesDir)).toEqual({ rocks_pmtiles: rocks, water_pmtiles: 'water' });
  });

  it('reports progress through a file by its size', async () => {
    const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'rocks' });
    const onProgress = vi.fn();
    await extractOakland({ file: zip }, { onProgress });
    expect(onProgress).toHaveBeenLastCalledWith({ loadedBytes: zip.size, totalBytes: zip.size });
  });

  it('unzips into the folder it is given', async () => {
    const zip = await packZip({ 'oakland/rocks.pmtiles': 'rocks' });
    const filesDir = newPackFilesDir('local:oakland');
    expect((await extractPack({ file: zip }, filesDir)).filesDir).toBe(filesDir);
    expect(await filesAsText(filesDir)).toEqual({ rocks_pmtiles: 'rocks' });
    expect(await versionFolders('local:oakland')).toHaveLength(1);
  });

  it('keeps the last of two files with the same name, like other unzip tools', async () => {
    const zip = await packZip({
      'us-ca-oakland/rocks.pmtiles': 'the first, longer rocks',
      'us-ca-oakland/old/rocks.pmtiles': 'later rocks',
    });
    const { filesDir } = await extractOakland({ file: zip });
    expect(await filesAsText(filesDir)).toEqual({ rocks_pmtiles: 'later rocks' });
  });

  it('removes what it unzipped when it is aborted', async () => {
    const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'rocks '.repeat(1000) });
    const ac = new AbortController();
    // Abort once the first chunk of the file is unzipped
    const result = extractOakland({ file: zip }, {
      onProgress: () => ac.abort(),
      signal: ac.signal,
    });
    await expect(result).rejects.toMatchObject({ name: 'AbortError' });
    expect(await versionFolders(OAKLAND)).toEqual([]);
  });

  it('still reports an abort when it can\'t remove what it unzipped', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(removePackFilesDir).mockRejectedValueOnce(
      new DOMException('The file is locked', 'NoModificationAllowedError'),
    );
    const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'rocks' });
    const ac = new AbortController();
    const result = extractOakland({ file: zip }, {
      onProgress: () => ac.abort(),
      signal: ac.signal,
    });
    await expect(result).rejects.toMatchObject({ name: 'AbortError' });
  });

  it('removes what it unzipped when the zip is cut off partway through a file', async () => {
    const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'rocks '.repeat(1000) });
    const cutOff = zip.slice(0, zip.size / 2);
    await expect(extractOakland({ file: cutOff })).rejects.toThrow();
    expect(await versionFolders(OAKLAND)).toEqual([]);
  });

  it('rejects a zip that is cut off between files', async () => {
    const zip = await packZip({
      'us-ca-oakland/rocks.pmtiles': 'rocks',
      'us-ca-oakland/water.pmtiles': 'water',
    });
    // jszip writes a folder entry first, so the water file starts at the third header
    const [, , waterStart] = await localHeaderOffsets(zip);
    const cutOff = zip.slice(0, waterStart);
    await expect(extractOakland({ file: cutOff })).rejects.toThrow(/cut off/);
    expect(await versionFolders(OAKLAND)).toEqual([]);
  });

  it('rejects a file that is not a pack zip', async () => {
    await expect(extractOakland({ file: new Blob(['not a zip']) })).rejects.toThrow();
    expect(await versionFolders(OAKLAND)).toEqual([]);
  });

  it('rejects a download the server failed to send', async () => {
    const notFound = new Response('Not Found', { status: 404 });
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve(notFound)));
    await expect(extractOakland({
      url: 'https://static.underfoot.rocks/us-ca-oakland.pmtiles.zip',
    })).rejects.toThrow(/404/);
    expect(await versionFolders(OAKLAND)).toEqual([]);
  });
});
