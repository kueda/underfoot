import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import WaterHeader from './WaterHeader';

describe('WaterHeader', () => {
  it('says how often a waterway has water', () => {
    render(
      <WaterHeader
        feature={{ layer: 'waterways', permanence: 'ephemeral', source: 'nhdplus_h_1502_hu4' }}
      />,
    );
    expect(screen.getByText('Ephemeral waterway')).toBeTruthy();
  });

  it('says how often a waterbody has water', () => {
    render(
      <WaterHeader
        feature={{ layer: 'waterbodies', permanence: 'intermittent', source: 'nhdplus_h_1502_hu4' }}
      />,
    );
    expect(screen.getByText('Intermittent waterbody')).toBeTruthy();
  });

  it('just names the layer when the source does not say how often it has water', () => {
    render(<WaterHeader feature={{ layer: 'waterways', source: 'tiger_water_35001' }} />);
    expect(screen.getByText('Waterway')).toBeTruthy();
  });

  // Like the flood pools behind dams, which are dry most of the time (#83)
  it('calls floodplains floodplains, not waterbodies', () => {
    render(
      <WaterHeader
        feature={{ layer: 'waterbodies', source: 'nhdplus_h_1204_hu4', type: 'floodplain' }}
      />,
    );
    expect(screen.getByText('Floodplain')).toBeTruthy();
  });

  it('calls the sea the ocean, since it has no name', () => {
    render(<WaterHeader feature={{ layer: 'waterbodies', source: 'osm', type: 'ocean/sea' }} />);
    expect(screen.getByText('Ocean')).toBeTruthy();
  });
});
