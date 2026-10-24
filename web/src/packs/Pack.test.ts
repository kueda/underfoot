import { describe, expect, it } from 'vitest';
import { packMetadata, packZip } from '../test/packFixtures';
import { extractPack } from './extractPack';
import { Pack } from './Pack';
import { newPackFilesDir } from './packFiles';
import { UnzippedPackData } from './types';

async function dataAsText(data: UnzippedPackData = {}) {
  const entries = await Promise.all(
    Object.entries(data).map(async ([key, blob]) => [key, await blob.text()]),
  );
  return Object.fromEntries(entries) as Record<string, string>;
}

// Unzips a pack with these files, like downloading it would
async function extract(files: Record<string, string>) {
  return extractPack({ file: await packZip(files) }, newPackFilesDir('us-ca-oakland'));
}

describe('Pack', () => {
  describe('fromExtracted', () => {
    it('reads the files that were unzipped', async () => {
      const extracted = await extract({
        'us-ca-oakland/rocks.pmtiles': 'rocks',
        'us-ca-oakland/water.pmtiles': 'water',
      });
      const pack = Pack.fromExtracted(packMetadata(), extracted);
      expect(pack.isDownloaded()).toBe(true);
      expect(await dataAsText(await pack.unzippedData())).toEqual({
        rocks_pmtiles: 'rocks',
        water_pmtiles: 'water',
      });
    });

    it('copies the manifest metadata onto the pack', () => {
      const pack = Pack.fromExtracted(packMetadata(), { filesDir: 'us-ca-oakland/v1' });
      expect(pack).toMatchObject({
        id: 'us-ca-oakland',
        name: 'Oakland',
        pmtilesPath: 'us-ca-oakland.pmtiles.zip',
        updatedAt: '2026-01-01T00:00:00.000Z',
      });
    });

    it('takes the default view from the zip\'s pack.json', () => {
      const pack = Pack.fromExtracted(packMetadata(), {
        filesDir: 'us-ak-southwest/v1',
        packJson: JSON.stringify({
          default_center: { lat: 60.79, lon: -161.76 },
          default_zoom: 10,
        }),
      });
      expect(pack.defaultCenter).toEqual({ lat: 60.79, lon: -161.76 });
      expect(pack.defaultZoom).toBe(10);
    });

    it.each([
      ['has no default view', {}],
      ['has a center without a lat', { default_center: { lon: -161.76 } }],
      ['has a center that isn\'t numbers', { default_center: { lat: '60', lon: '-161' } }],
      ['has a zoom that isn\'t a number', { default_zoom: '10' }],
    ])('leaves out the default view when pack.json %s', (_case, packJson) => {
      const pack = Pack.fromExtracted(packMetadata(), {
        filesDir: 'us-ak-southwest/v1',
        packJson: JSON.stringify(packJson),
      });
      expect(pack.defaultCenter).toBeUndefined();
      expect(pack.defaultZoom).toBeUndefined();
    });
  });

  describe('fromLocalExtracted', () => {
    const FILE_NAME = 'us-ca-oakland.pmtiles.zip';
    const FILES_DIR = 'local%3Aus-ca-oakland/v1';

    it('takes the name and description from the zip\'s pack.json but keeps the local id', () => {
      const pack = Pack.fromLocalExtracted(FILE_NAME, {
        filesDir: FILES_DIR,
        packJson: JSON.stringify(packMetadata({
          description: 'Oakland, CA, USA. Mostly for testing some place small.',
          name: 'Oakland, CA, USA',
        })),
      });
      expect(pack).toMatchObject({
        description: 'Oakland, CA, USA. Mostly for testing some place small.',
        filesDir: FILES_DIR,
        id: 'local:us-ca-oakland',
        name: 'Oakland, CA, USA',
        sourceFileName: FILE_NAME,
      });
    });

    it('names the pack after the file when the zip has no pack.json', () => {
      const pack = Pack.fromLocalExtracted(FILE_NAME, { filesDir: FILES_DIR });
      expect(pack).toMatchObject({
        description: '',
        id: 'local:us-ca-oakland',
        name: 'us-ca-oakland',
      });
    });

    it.each([
      ['isn\'t JSON', 'not json'],
      ['isn\'t an object', '[]'],
      ['has an empty name', JSON.stringify({ name: '', description: '' })],
      ['has a name that isn\'t a string', JSON.stringify({ name: 5, description: 5 })],
    ])('names the pack after the file when pack.json %s', (_case, packJson) => {
      const pack = Pack.fromLocalExtracted(FILE_NAME, { filesDir: FILES_DIR, packJson });
      expect(pack).toMatchObject({
        description: '',
        name: 'us-ca-oakland',
      });
    });

    it('falls back to the file name for just the field pack.json lacks', () => {
      const pack = Pack.fromLocalExtracted(FILE_NAME, {
        filesDir: FILES_DIR,
        packJson: JSON.stringify({ name: 'Oakland, CA, USA' }),
      });
      expect(pack).toMatchObject({
        description: '',
        name: 'Oakland, CA, USA',
      });
    });

    it('takes the default view from the zip\'s pack.json', () => {
      const pack = Pack.fromLocalExtracted(FILE_NAME, {
        filesDir: FILES_DIR,
        packJson: JSON.stringify({
          default_center: { lat: 37.8, lon: -122.27 },
          default_zoom: 12,
        }),
      });
      expect(pack.defaultCenter).toEqual({ lat: 37.8, lon: -122.27 });
      expect(pack.defaultZoom).toBe(12);
    });
  });

  describe('fromPack', () => {
    it('turns a pack read back from IndexedDB into a Pack again', async () => {
      const extracted = await extract({ 'us-ca-oakland/rocks.pmtiles': 'rocks' });
      const original = Pack.fromExtracted(packMetadata(), extracted);
      original.downloadedAt = '2026-01-15T00:00:00.000Z';
      // IndexedDB stores a copy made this way, which keeps the fields but not the class
      const stored = structuredClone(original);
      expect(stored).not.toBeInstanceOf(Pack);

      const pack = Pack.fromPack(stored);
      expect(pack).toBeInstanceOf(Pack);
      expect(pack).toEqual(original);
      expect(await dataAsText(await pack.unzippedData())).toEqual({ rocks_pmtiles: 'rocks' });
    });

    it('reads a pack an older version of the app stored with its files in IndexedDB', async () => {
      const original = new Pack(packMetadata(), { rocks_pmtiles: new Blob(['rocks']) });
      const pack = Pack.fromPack(structuredClone(original));
      expect(pack.isDownloaded()).toBe(true);
      expect(await dataAsText(await pack.unzippedData())).toEqual({ rocks_pmtiles: 'rocks' });
    });

    it('keeps the file name a local pack was loaded from', () => {
      const original = Pack.fromLocalExtracted('us-ca-oakland.pmtiles.zip', {
        filesDir: 'local%3Aus-ca-oakland/v1',
      });
      const pack = Pack.fromPack(structuredClone(original));
      expect(pack.sourceFileName).toBe('us-ca-oakland.pmtiles.zip');
    });

    it('keeps the default view', () => {
      const original = Pack.fromExtracted(packMetadata(), {
        filesDir: 'us-ca-oakland/v1',
        packJson: JSON.stringify({
          default_center: { lat: 37.8, lon: -122.27 },
          default_zoom: 12,
        }),
      });
      const pack = Pack.fromPack(structuredClone(original));
      expect(pack.defaultCenter).toEqual({ lat: 37.8, lon: -122.27 });
      expect(pack.defaultZoom).toBe(12);
    });
  });

  describe('isDownloaded', () => {
    it('is false for a pack that only has manifest metadata', () => {
      expect(new Pack(packMetadata()).isDownloaded()).toBe(false);
    });
  });

  describe('metadataFromFileName', () => {
    it.each([
      ['Oakland Hills.pmtiles.zip', 'local:oakland-hills'],
      ['us-ca-oakland.zip', 'local:us-ca-oakland'],
      ['--Mt. Diablo (2026)--.PMTILES.ZIP', 'local:mt-diablo-2026'],
      ['!!!.zip', 'local:pack'],
    ])('gives %j the id %j', (fileName, id) => {
      expect(Pack.metadataFromFileName(fileName).id).toBe(id);
    });

    it('names the pack after the file', () => {
      expect(Pack.metadataFromFileName('Oakland Hills.pmtiles.zip')).toMatchObject({
        description: '',
        name: 'oakland-hills',
      });
    });
  });
});
