import type { Map, StyleSpecification, VectorSourceSpecification } from 'maplibre-gl';
import Papa from 'papaparse';
import * as pmtiles from 'pmtiles';
import { describe, expect, it, vi } from 'vitest';

import type { UnzippedPackData } from '../packs/types';
import { ROCK_STYLE, SEA_LAYER_ID } from './mapStyles';
import { loadMapFromPackData, packStartView } from './util';

// Papa can't read Files under jsdom, and parsing isn't what these test
vi.mock('papaparse', () => ({ default: { parse: vi.fn() } }));

vi.mock('pmtiles', () => ({
  FileSource: vi.fn(),
  PMTiles: vi.fn(),
}));

function mockTilesMaxZoom(maxZoom: number) {
  vi.mocked(pmtiles.PMTiles).mockImplementation(function () {
    return { getHeader: () => Promise.resolve({ maxZoom }) } as unknown as pmtiles.PMTiles;
  });
}

async function loadRocksStyle(
  contoursMaxZoom = 14,
  packData: UnzippedPackData = {
    rocks_pmtiles: new Blob(['tiles']),
    rocks_citations_csv: new Blob(['source,citation\n']),
  },
  protocol = { add: vi.fn() },
) {
  const setStyle = vi.fn();
  await loadMapFromPackData(
    packData,
    protocol as unknown as pmtiles.Protocol,
    { setStyle } as unknown as Map,
    'rocks',
    vi.fn(),
    vi.fn(),
    contoursMaxZoom,
  );
  return setStyle.mock.calls[0][0] as StyleSpecification;
}

async function loadRocks() {
  return (await loadRocksStyle()).sources.rocks as VectorSourceSpecification;
}

describe('loadMapFromPackData', () => {
  it('overzooms rocks past the maxzoom of the pack\'s tiles', async () => {
    mockTilesMaxZoom(10);
    expect((await loadRocks()).maxzoom).toBe(10);
  });

  it('leaves the shared rock style alone', async () => {
    mockTilesMaxZoom(10);
    await loadRocks();
    expect((ROCK_STYLE.sources.rocks as VectorSourceSpecification).maxzoom).toBe(14);
  });

  it('overzooms contours past the maxzoom of the pack\'s contour tiles', async () => {
    mockTilesMaxZoom(10);
    const style = await loadRocksStyle(12);
    expect((style.sources.contours as VectorSourceSpecification).maxzoom).toBe(12);
  });
});

describe('loadMapFromPackData with water tiles on the rocks map', () => {
  const packData: UnzippedPackData = {
    rocks_pmtiles: new Blob(['rocks']),
    rocks_citations_csv: new Blob(['source,citation\n']),
    water_pmtiles: new Blob(['water']),
    water_citations_csv: new Blob(['source,citation\n']),
  };

  it('adds the water tiles for the sea under the rocks', async () => {
    mockTilesMaxZoom(10);
    const protocol = { add: vi.fn() };
    const style = await loadRocksStyle(14, packData, protocol);
    expect(protocol.add).toHaveBeenCalledTimes(2);
    expect(vi.mocked(pmtiles.FileSource).mock.calls.map(([file]) => file.name))
      .toEqual(expect.arrayContaining(['rocks', 'water']));
    expect(style.sources.water).toBeDefined();
    expect(style.layers.map(l => l.id)).toContain(SEA_LAYER_ID);
  });

  it('leaves the sea out for packs without water tiles', async () => {
    mockTilesMaxZoom(10);
    const style = await loadRocksStyle();
    expect(style.sources.water).toBeUndefined();
    expect(style.layers.map(l => l.id)).not.toContain(SEA_LAYER_ID);
  });

  it('reads the water citations too, for the sea\'s source', async () => {
    mockTilesMaxZoom(10);
    vi.mocked(Papa.parse).mockClear();
    await loadRocksStyle(14, packData);
    const names = vi.mocked(Papa.parse).mock.calls.map(([file]) => (file as unknown as File).name);
    expect(names).toEqual(['rocks_citations', 'water_citations']);
  });
});

describe('packStartView', () => {
  const waysHeader = { centerLat: 57.35, centerLon: -165.12, maxZoom: 13 };

  it('starts at the center of the ways tiles, zoomed out from their maxzoom', () => {
    expect(packStartView({}, waysHeader)).toEqual({ center: [-165.12, 57.35], zoom: 11 });
  });

  it('starts at the pack\'s default center and zoom', () => {
    expect(packStartView(
      { defaultCenter: { lat: 60.79, lon: -161.76 }, defaultZoom: 10 },
      waysHeader,
    )).toEqual({ center: [-161.76, 60.79], zoom: 10 });
  });

  it('starts at the pack\'s default center even without a default zoom', () => {
    expect(packStartView({ defaultCenter: { lat: 60.79, lon: -161.76 } }, waysHeader))
      .toEqual({ center: [-161.76, 60.79], zoom: 11 });
  });
});
