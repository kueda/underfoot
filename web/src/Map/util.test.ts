import type { Map, StyleSpecification, VectorSourceSpecification } from 'maplibre-gl';
import * as pmtiles from 'pmtiles';
import { describe, expect, it, vi } from 'vitest';

import { ROCK_STYLE } from './mapStyles';
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

async function loadRocksStyle(contoursMaxZoom = 14) {
  const setStyle = vi.fn();
  await loadMapFromPackData(
    {
      rocks_pmtiles: new Blob(['tiles']),
      rocks_citations_csv: new Blob(['source,citation\n']),
    },
    { add: vi.fn() } as unknown as pmtiles.Protocol,
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
