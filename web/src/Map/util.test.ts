import type { Map, StyleSpecification, VectorSourceSpecification } from 'maplibre-gl';
import * as pmtiles from 'pmtiles';
import { describe, expect, it, vi } from 'vitest';

import { ROCK_STYLE } from './mapStyles';
import { loadMapFromPackData } from './util';

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

async function loadRocks() {
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
  );
  const style = setStyle.mock.calls[0][0] as StyleSpecification;
  return style.sources.rocks as VectorSourceSpecification;
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
});
