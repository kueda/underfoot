import { describe, expect, it } from 'vitest';

import { TRACE_FADING_PAINT, WATER_STYLE } from './mapStyles';

function layer(id: string) {
  const found = WATER_STYLE.layers.find(l => l.id === id);
  if (!found) throw new Error(`No ${id} layer in WATER_STYLE`);
  return found;
}

describe('TRACE_FADING_PAINT', () => {
  it('restores the colors the water style draws with when a trace clears', () => {
    for (const { layer: id, property, color } of TRACE_FADING_PAINT) {
      const paint = (layer(id) as { paint?: Record<string, unknown> }).paint ?? {};
      expect(paint[property], `${id} ${property}`).toEqual(color);
    }
  });

  it('fades every road layer', () => {
    const roadLayers = WATER_STYLE.layers
      .filter(l => 'source' in l && l.source === 'ways')
      .map(l => l.id);
    const fadedLayers = TRACE_FADING_PAINT.map(p => p.layer);
    expect(roadLayers.length).toBeGreaterThan(0);
    expect(fadedLayers).toEqual(expect.arrayContaining(roadLayers));
  });

  it('fades waterways and waterbodies', () => {
    expect(TRACE_FADING_PAINT.map(p => p.layer)).toEqual(
      expect.arrayContaining(['waterways', 'waterbodies']),
    );
  });
});
