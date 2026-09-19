import type { StyleSpecification } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';

import { ROCK_STYLE, TRACE_FADING_PAINT, WATER_STYLE } from './mapStyles';

function layer(id: string, style: StyleSpecification = WATER_STYLE) {
  const found = style.layers.find(l => l.id === id);
  if (!found) throw new Error(`No ${id} layer in style`);
  return found;
}

function paint(id: string, property: string, style: StyleSpecification = WATER_STYLE) {
  return ((layer(id, style) as { paint?: Record<string, unknown> }).paint ?? {})[property];
}

// Relative lightness of a #rrggbb or rgb(r,g,b) color, from 0 to 765
function lightness(color: unknown) {
  const text = String(color);
  const channels = text.startsWith('#')
    ? [1, 3, 5].map(i => parseInt(text.slice(i, i + 2), 16))
    : (text.match(/\d+/g) ?? []).map(Number);
  expect(channels, `${text} should be a color`).toHaveLength(3);
  return channels.reduce((sum, channel) => sum + channel, 0);
}

const ROAD_LINE_LAYERS = ['ways', 'highways', 'roads', 'trails'];

describe('WATER_STYLE', () => {
  it('draws roads lighter than the rocks map so they compete less with the water', () => {
    for (const id of ROAD_LINE_LAYERS) {
      expect(lightness(paint(id, 'line-color')), id)
        .toBeGreaterThan(lightness(paint(id, 'line-color', ROCK_STYLE)));
    }
    expect(lightness(paint('ways-labels', 'text-color')))
      .toBeGreaterThan(lightness(paint('ways-labels', 'text-color', ROCK_STYLE)));
  });
});

describe('TRACE_FADING_PAINT', () => {
  it('restores the colors the water style draws with when a trace clears', () => {
    for (const { layer: id, property, color } of TRACE_FADING_PAINT) {
      expect(paint(id, property), `${id} ${property}`).toEqual(color);
    }
  });

  it('fades roads further while tracing', () => {
    for (const { layer: id, color, faded } of TRACE_FADING_PAINT) {
      if (!ROAD_LINE_LAYERS.includes(id) && id !== 'ways-labels') continue;
      expect(lightness(faded), id).toBeGreaterThan(lightness(color));
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
