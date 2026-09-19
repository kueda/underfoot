import { describe, expect, it } from 'vitest';
import { packMetadata, packZip } from '../test/packFixtures';
import { Pack } from './Pack';
import { UnzippedPackData } from './types';

async function dataAsText(data: UnzippedPackData = {}) {
  const entries = await Promise.all(
    Object.entries(data).map(async ([key, blob]) => [key, await blob.text()]),
  );
  return Object.fromEntries(entries) as Record<string, string>;
}

describe('Pack', () => {
  describe('fromZip', () => {
    it('reads each known file from a folder in the zip and ignores other files', async () => {
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
      const pack = await Pack.fromZip(packMetadata(), zip);
      expect(await dataAsText(pack.data)).toEqual({
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

    it('copies the manifest metadata onto the pack', async () => {
      const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'rocks' });
      const pack = await Pack.fromZip(packMetadata(), zip);
      expect(pack).toMatchObject({
        id: 'us-ca-oakland',
        name: 'Oakland',
        pmtilesPath: 'us-ca-oakland.pmtiles.zip',
        updatedAt: '2026-01-01T00:00:00.000Z',
      });
    });
  });

  describe('fromLocalZip', () => {
    const FILE_NAME = 'us-ca-oakland.pmtiles.zip';

    it('takes the name and description from the zip\'s pack.json but keeps the local id', async () => {
      const zip = await packZip({
        'us-ca-oakland.pmtiles/pack.json': JSON.stringify(packMetadata({
          description: 'Oakland, CA, USA. Mostly for testing some place small.',
          name: 'Oakland, CA, USA',
        })),
        'us-ca-oakland.pmtiles/rocks.pmtiles': 'rocks',
      });
      const pack = await Pack.fromLocalZip(FILE_NAME, zip);
      expect(pack).toMatchObject({
        description: 'Oakland, CA, USA. Mostly for testing some place small.',
        id: 'local:us-ca-oakland',
        name: 'Oakland, CA, USA',
        sourceFileName: FILE_NAME,
      });
      expect(await dataAsText(pack.data)).toEqual({ rocks_pmtiles: 'rocks' });
    });

    it('names the pack after the file when the zip has no pack.json', async () => {
      const zip = await packZip({ 'us-ca-oakland.pmtiles/rocks.pmtiles': 'rocks' });
      const pack = await Pack.fromLocalZip(FILE_NAME, zip);
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
    ])('names the pack after the file when pack.json %s', async (_case, packJson) => {
      const zip = await packZip({
        'us-ca-oakland.pmtiles/pack.json': packJson,
        'us-ca-oakland.pmtiles/rocks.pmtiles': 'rocks',
      });
      const pack = await Pack.fromLocalZip(FILE_NAME, zip);
      expect(pack).toMatchObject({
        description: '',
        name: 'us-ca-oakland',
      });
      expect(await dataAsText(pack.data)).toEqual({ rocks_pmtiles: 'rocks' });
    });

    it('falls back to the file name for just the field pack.json lacks', async () => {
      const zip = await packZip({
        'us-ca-oakland.pmtiles/pack.json': JSON.stringify({ name: 'Oakland, CA, USA' }),
      });
      const pack = await Pack.fromLocalZip(FILE_NAME, zip);
      expect(pack).toMatchObject({
        description: '',
        name: 'Oakland, CA, USA',
      });
    });
  });

  describe('fromPack', () => {
    it('turns a pack read back from IndexedDB into a Pack again', async () => {
      const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'rocks' });
      const original = await Pack.fromZip(packMetadata(), zip);
      original.downloadedAt = '2026-01-15T00:00:00.000Z';
      // IndexedDB stores a copy made this way, which keeps the fields but not the class
      const stored = structuredClone(original);
      expect(stored).not.toBeInstanceOf(Pack);

      const pack = Pack.fromPack(stored);
      expect(pack).toBeInstanceOf(Pack);
      expect(pack).toEqual(original);
      expect(await dataAsText(await pack.unzippedData())).toEqual({ rocks_pmtiles: 'rocks' });
    });

    it('keeps the file name a local pack was loaded from', async () => {
      const zip = await packZip({ 'us-ca-oakland/rocks.pmtiles': 'rocks' });
      const original = await Pack.fromLocalZip('us-ca-oakland.pmtiles.zip', zip);
      const pack = Pack.fromPack(structuredClone(original));
      expect(pack.sourceFileName).toBe('us-ca-oakland.pmtiles.zip');
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
