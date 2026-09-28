import { expression, featureFilter } from '@maplibre/maplibre-gl-style-spec';
import type { FilterSpecification, StyleSpecification } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';

import { TRACE_DIRECTIONS, TRACE_LAYER_IDS } from './flowTrace';
import {
  MARSH_PATTERNS,
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

// Evaluate a data-driven style value for a waterway with the given properties
function evaluateForWaterway(value: unknown, properties: Record<string, unknown>): unknown {
  const parsed = expression.createExpression(value, 'style-property');
  if (parsed.result !== 'success') throw new Error('value should be an expression');
  return parsed.value.evaluate({ zoom: 14 }, { type: 'LineString', properties });
}

const STREAM = { is_natural: 1, type: 'stream' };
const CANAL = { is_natural: 0, type: 'canal/ditch' };
// NHD draws these through lakes and wide rivers to carry flow across them
const ARTIFICIAL_PATH = { is_natural: 0, type: 'artificial' };

// Evaluate a data-driven style value for a waterbody with the given properties
function evaluateForWaterbody(value: unknown, properties: Record<string, unknown>): unknown {
  const parsed = expression.createExpression(value, 'style-property');
  if (parsed.result !== 'success') throw new Error('value should be an expression');
  return parsed.value.evaluate({ zoom: 14 }, { type: 'Polygon', properties });
}

const LAKE = { is_natural: 1, type: 'lake/pond' };
const SWAMP = { is_natural: 1, type: 'swamp/marsh' };

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

describe('waterway colors', () => {
  const colorProperties = [
    ['waterways', 'line-color'],
    [WATERWAY_ARROWS_LAYER_ID, 'text-color'],
    ['waterways-labels', 'text-color'],
  ];

  it('draw artificial paths through waterbodies like natural water', () => {
    for (const [id, property] of colorProperties) {
      const color = paint(id, property);
      expect(evaluateForWaterway(color, ARTIFICIAL_PATH), `${id} ${property}`)
        .toEqual(evaluateForWaterway(color, STREAM));
    }
  });

  it('draw other man-made waterways in their own color', () => {
    for (const [id, property] of colorProperties) {
      const color = paint(id, property);
      expect(evaluateForWaterway(color, CANAL), `${id} ${property}`)
        .not.toEqual(evaluateForWaterway(color, STREAM));
    }
  });

  it('fade artificial paths like natural water while tracing', () => {
    const fading = TRACE_FADING_PAINT.filter(
      p => p.layer === 'waterways' || p.layer === WATERWAY_ARROWS_LAYER_ID,
    );
    expect(fading).toHaveLength(2);
    for (const { layer: id, faded } of fading) {
      expect(evaluateForWaterway(faded, ARTIFICIAL_PATH), id)
        .toEqual(evaluateForWaterway(faded, STREAM));
      expect(evaluateForWaterway(faded, CANAL), id)
        .not.toEqual(evaluateForWaterway(faded, STREAM));
    }
  });
});

describe('waterbody colors', () => {
  const waterbodyFading = () => {
    const fading = TRACE_FADING_PAINT.find(p => p.layer === 'waterbodies');
    if (!fading) throw new Error('waterbodies should fade while tracing');
    return fading;
  };

  it('draw swamps and marshes lighter than open water', () => {
    const color = paint('waterbodies', 'fill-color');
    expect(lightness(evaluateForWaterbody(color, SWAMP)))
      .toBeGreaterThan(lightness(evaluateForWaterbody(color, LAKE)));
  });

  it('keep swamps and marshes lighter than open water while tracing', () => {
    const { faded } = waterbodyFading();
    expect(lightness(evaluateForWaterbody(faded, SWAMP)))
      .toBeGreaterThan(lightness(evaluateForWaterbody(faded, LAKE)));
  });

  it('fade swamps and marshes further while tracing', () => {
    const { color, faded } = waterbodyFading();
    expect(lightness(evaluateForWaterbody(faded, SWAMP)))
      .toBeGreaterThan(lightness(evaluateForWaterbody(color, SWAMP)));
  });
});

describe('marsh pattern', () => {
  const MARSH_LAYER_ID = 'waterbodies-marsh';

  it('draws tufts over swamps and marshes only', () => {
    const marsh = layer(MARSH_LAYER_ID) as {
      'type': string;
      'source-layer'?: string;
      'filter'?: FilterSpecification;
    };
    expect(marsh.type).toBe('fill');
    expect(marsh['source-layer']).toBe('waterbodies');
    if (!marsh.filter) throw new Error('marsh layer should have a filter');
    const { filter: evaluate } = featureFilter(marsh.filter, 'filter');
    const draws = (properties: Record<string, unknown>) => evaluate(
      { zoom: 14 },
      { type: 'Polygon', properties },
    );
    expect(draws(SWAMP)).toBe(true);
    expect(draws(LAKE)).toBe(false);
  });

  it('hides tufts when zoomed out, where they get too busy', () => {
    expect((layer(MARSH_LAYER_ID) as { minzoom?: number }).minzoom).toBe(12);
  });

  it('draws tufts over the swamp fill, under waterways', () => {
    const ids = WATER_STYLE.layers.map(l => l.id);
    expect(ids.indexOf(MARSH_LAYER_ID)).toBeGreaterThan(ids.indexOf('waterbodies'));
    expect(ids.indexOf(MARSH_LAYER_ID)).toBeLessThan(ids.indexOf('waterways'));
  });

  it('draws tufts lighter than open water but darker than the swamp fill', () => {
    const fill = paint('waterbodies', 'fill-color');
    const tufts = lightness(MARSH_PATTERNS[String(paint(MARSH_LAYER_ID, 'fill-pattern'))]);
    expect(tufts).toBeGreaterThan(lightness(evaluateForWaterbody(fill, LAKE)));
    expect(tufts).toBeLessThan(lightness(evaluateForWaterbody(fill, SWAMP)));
  });

  it('uses a pattern the map knows how to draw', () => {
    expect(Object.keys(MARSH_PATTERNS)).toContain(paint(MARSH_LAYER_ID, 'fill-pattern'));
  });

  it('swaps in lighter tufts while tracing', () => {
    const fading = TRACE_FADING_PAINT.find(p => p.layer === MARSH_LAYER_ID);
    if (!fading) throw new Error('marsh tufts should fade while tracing');
    expect(fading.property).toBe('fill-pattern');
    const { color: pattern, faded: fadedPattern } = fading;
    expect(Object.keys(MARSH_PATTERNS)).toContain(fadedPattern);
    expect(lightness(MARSH_PATTERNS[String(fadedPattern)]))
      .toBeGreaterThan(lightness(MARSH_PATTERNS[String(pattern)]));
  });
});

// A line is solid when none of the gaps in its dash pattern have any length
function isSolid(dasharray: unknown) {
  expect(Array.isArray(dasharray), `${String(dasharray)} should be a dash pattern`).toBe(true);
  return (dasharray as number[]).every((length, i) => i % 2 === 0 || length === 0);
}

describe('waterway permanence', () => {
  const dasharray = (properties: Record<string, unknown>) => evaluateForWaterway(
    paint('waterways', 'line-dasharray'),
    properties,
  );
  const width = (properties: Record<string, unknown>) => evaluateForWaterway(
    paint('waterways', 'line-width'),
    properties,
  );

  it('draws perennial waterways solid', () => {
    expect(isSolid(dasharray({ ...STREAM, permanence: 'perennial' }))).toBe(true);
  });

  it('draws waterways with unknown permanence solid', () => {
    // TIGER and some NHD features don't say how often they flow
    expect(isSolid(dasharray(STREAM))).toBe(true);
  });

  it('dashes intermittent and ephemeral waterways', () => {
    expect(isSolid(dasharray({ ...STREAM, permanence: 'intermittent' }))).toBe(false);
    expect(isSolid(dasharray({ ...STREAM, permanence: 'ephemeral' }))).toBe(false);
  });

  it('dashes ephemeral waterways differently than intermittent ones', () => {
    expect(dasharray({ ...STREAM, permanence: 'ephemeral' }))
      .not.toEqual(dasharray({ ...STREAM, permanence: 'intermittent' }));
  });

  it('draws ephemeral waterways as round dots so they do not look like dashed trails', () => {
    const { layout } = layer('waterways') as { layout?: Record<string, unknown> };
    const ephemeral = { ...STREAM, permanence: 'ephemeral' };
    const dashes = (dasharray(ephemeral) as number[]).filter((_, i) => i % 2 === 0);
    expect(dashes.every(length => length === 0)).toBe(true);
    expect(evaluateForWaterway(layout?.['line-cap'], ephemeral)).toBe('round');
  });

  it('draws intermittent waterways dash-dot so they do not look like dashed trails', () => {
    const { layout } = layer('waterways') as { layout?: Record<string, unknown> };
    const intermittent = { ...STREAM, permanence: 'intermittent' };
    const dashes = (dasharray(intermittent) as number[]).filter((_, i) => i % 2 === 0);
    expect(dashes.some(length => length > 0)).toBe(true);
    expect(dashes).toContain(0);
    expect(evaluateForWaterway(layout?.['line-cap'], intermittent)).toBe('round');
  });

  it('draws ephemeral waterways thinner so dense desert networks read lighter', () => {
    expect(width({ ...STREAM, permanence: 'ephemeral' }))
      .toBeLessThan(width({ ...STREAM, permanence: 'perennial' }) as number);
  });

  it('dashes artificial paths through intermittent waterbodies', () => {
    expect(isSolid(dasharray({ ...ARTIFICIAL_PATH, permanence: 'intermittent' }))).toBe(false);
  });
});

describe('waterbody permanence', () => {
  const INTERMITTENT_OUTLINE_LAYER_ID = 'waterbodies-intermittent-outline';

  const opacity = (properties: Record<string, unknown>) => evaluateForWaterway(
    paint('waterbodies', 'fill-opacity'),
    properties,
  );

  it('fills intermittent waterbodies lighter than perennial ones', () => {
    expect(opacity({ permanence: 'intermittent' }))
      .toBeLessThan(opacity({ permanence: 'perennial' }) as number);
  });

  it('fills waterbodies with unknown permanence like perennial ones', () => {
    expect(opacity({})).toEqual(opacity({ permanence: 'perennial' }));
  });

  it('outlines only intermittent waterbodies with a dashed line', () => {
    const outline = layer(INTERMITTENT_OUTLINE_LAYER_ID) as {
      type: string;
      filter?: FilterSpecification;
      layout?: Record<string, unknown>;
      paint?: Record<string, unknown>;
    };
    expect(outline.type).toBe('line');
    expect(isSolid(outline.paint?.['line-dasharray'])).toBe(false);
    // Round caps draw the dots in the intermittent dash-dot pattern
    expect(outline.layout?.['line-cap']).toBe('round');
    const { filter: evaluate } = featureFilter(outline.filter, 'filter');
    const draws = (properties: Record<string, unknown>) => evaluate(
      { zoom: 14 },
      { type: 'Polygon', properties },
    );
    expect(draws({ permanence: 'intermittent' })).toBe(true);
    expect(draws({ permanence: 'perennial' })).toBe(false);
    expect(draws({})).toBe(false);
  });

  it('fades the intermittent outline while tracing', () => {
    expect(TRACE_FADING_PAINT.map(p => p.layer)).toContain(INTERMITTENT_OUTLINE_LAYER_ID);
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

  describe('glyphs', () => {
    const { layout } = layer(WATERWAY_ARROWS_LAYER_ID) as { layout?: Record<string, unknown> };
    const arrow = (properties: Record<string, unknown>): unknown => evaluateForWaterway(
      layout?.['text-field'],
      { ...properties, flow_pre: 3, flow_upstream: 1 },
    );

    it('differ between natural and man-made waterways so they keep their own colors', () => {
      // MapLibre joins connected lines with the same text into one line that
      // keeps only one of their colors, so a culvert's orange could spread to
      // the natural creek it's part of
      expect(arrow(STREAM)).toBeTruthy();
      expect(arrow(CANAL)).toBeTruthy();
      expect(arrow(CANAL)).not.toEqual(arrow(STREAM));
    });

    it('are the same for artificial paths, which are drawn like natural water', () => {
      expect(arrow(ARTIFICIAL_PATH)).toEqual(arrow(STREAM));
    });
  });

  it('show along traces, which set the same filter on their lines and arrows', () => {
    for (const direction of TRACE_DIRECTIONS) {
      const { line, arrows } = TRACE_LAYER_IDS[direction];
      expect(layer(line).type, line).toBe('line');
      expect(layer(arrows).type, arrows).toBe('symbol');
    }
  });
});
