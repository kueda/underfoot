import { render, screen } from '@testing-library/react';
import type { Map } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';

import type { RockUnit } from '../../packs/types';
import OsmEditLink from './OsmEditLink';

const map = {
  getZoom: () => 13.4,
  getCenter: () => ({ lat: 38.1809712, lng: -122.6705634 }),
} as unknown as Map;

describe('OsmEditLink', () => {
  it('links features from OpenStreetMap to editing OSM where the map is', () => {
    render(<OsmEditLink feature={{ source: 'osm' }} map={map} />);
    const link = screen.getByRole('link', { name: 'edit this!' });
    // OSM's zoom levels are one more than MapLibre's for the same area
    expect(link.getAttribute('href'))
      .toBe('https://www.openstreetmap.org/edit#map=14/38.18097/-122.67056');
  });

  it('links any kind of feature from OpenStreetMap', () => {
    const rockUnit: RockUnit = { source: 'osm', id: 1, code: 'Qal', lithology: 'alluvium' };
    render(<OsmEditLink feature={rockUnit} map={map} />);
    expect(screen.getByRole('link', { name: 'edit this!' })).toBeTruthy();
  });

  it('does not link features from other sources', () => {
    render(<OsmEditLink feature={{ source: 'nhdplus_h_1805_hu4' }} map={map} />);
    expect(screen.queryByRole('link', { name: 'edit this!' })).toBeNull();
  });
});
