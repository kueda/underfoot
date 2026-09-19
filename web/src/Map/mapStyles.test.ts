import { expression, featureFilter } from '@maplibre/maplibre-gl-style-spec';
import type { FilterSpecification, StyleSpecification } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';

import { TRACE_DIRECTIONS, TRACE_LAYER_IDS } from './flowTrace';
import {
  ROCK_STYLE,
  TRACE_FADING_PAINT,
  WATER_STYLE,
  WATERWAY_ARROWS_LAYER_ID,
} from './mapStyles';

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
      expect.arrayContaining(['waterways', 'waterbodies', WATERWAY_ARROWS_LAYER_ID]),
    );
  });
});

describe('flow direction arrows', () => {
  const arrowLayerIds = [
    WATERWAY_ARROWS_LAYER_ID,
    ...TRACE_DIRECTIONS.map(direction => TRACE_LAYER_IDS[direction].arrows),
  ];

  it('stay pointed downstream instead of flipping to stay upright', () => {
    // Waterways are drawn from upstream to downstream, and MapLibre would
    // otherwise turn arrows on westward waterways around so they read upright
    for (const id of arrowLayerIds) {
      const { layout } = layer(id) as { layout?: Record<string, unknown> };
      expect(layout?.['symbol-placement'], id).toBe('line');
      expect(layout?.['text-keep-upright'], id).toBe(false);
    }
  });

  it('only point along waterways with flow labels', () => {
    // Waterways from other sources, like TIGER, aren't drawn in the direction of flow
    const { filter } = layer(WATERWAY_ARROWS_LAYER_ID) as { filter?: FilterSpecification };
    const { filter: evaluate } = featureFilter(filter, 'filter');
    const draws = (properties: Record<string, unknown>) => evaluate(
      { zoom: 14 },
      { type: 'LineString', properties },
    );
    expect(draws({ flow_pre: 3, flow_upstream: 1 })).toBe(true);
    expect(draws({ name: 'Temescal Creek' })).toBe(false);
  });

  it('differ between natural and artificial waterways so they keep their own colors', () => {
    // MapLibre joins connected lines with the same text into one line that
    // keeps only one of their colors, so a culvert's orange could spread to
    // the natural creek it's part of
    const { layout } = layer(WATERWAY_ARROWS_LAYER_ID) as { layout?: Record<string, unknown> };
    const parsed = expression.createExpression(layout?.['text-field'], 'text-field');
    if (parsed.result !== 'success') throw new Error('text-field should be an expression');
    const arrow = (isNatural: number): unknown => parsed.value.evaluate(
      { zoom: 14 },
      { type: 'LineString', properties: { is_natural: isNatural, flow_pre: 3, flow_upstream: 1 } },
    );
    expect(arrow(1)).toBeTruthy();
    expect(arrow(0)).toBeTruthy();
    expect(arrow(0)).not.toEqual(arrow(1));
  });

  it('show along traces, which set the same filter on their lines and arrows', () => {
    for (const direction of TRACE_DIRECTIONS) {
      const { line, arrows } = TRACE_LAYER_IDS[direction];
      expect(layer(line).type, line).toBe('line');
      expect(layer(arrows).type, arrows).toBe('symbol');
    }
  });
});
