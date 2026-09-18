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
        // Named after its waterways_network table in data/water.py, hence the underscore
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
        water_waterways_network_csv: 'waterways network',
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
        description: 'Loaded from Oakland Hills.pmtiles.zip',
        name: 'oakland-hills',
      });
    });
  });
});
