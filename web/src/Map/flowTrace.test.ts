import { featureFilter } from '@maplibre/maplibre-gl-style-spec';
import type { FilterSpecification } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';

import { downstreamFilter, flowPre, NO_TRACE_FILTER } from './flowTrace';

// Waterways labeled the way data/water.py's label_flow_tree labels them. The
// headwater "c" and the tributary "d" both flow into "b", "e" flows into "d",
// and "b" flows into the outlet "a". "x" and "y" are a separate river, and
// "tiger" is a waterway from a source without flow data.
const WATERWAYS: Record<string, Record<string, number>> = {
  a: { flow_pre: 0, flow_upstream: 4 },
  b: { flow_pre: 1, flow_upstream: 3 },
  c: { flow_pre: 2, flow_upstream: 0 },
  d: { flow_pre: 3, flow_upstream: 1 },
  e: { flow_pre: 4, flow_upstream: 0 },
  x: { flow_pre: 5, flow_upstream: 1 },
  y: { flow_pre: 6, flow_upstream: 0 },
  tiger: {},
};

// Names of the waterways a map layer with this filter would draw
function matching(filter: FilterSpecification) {
  const { filter: evaluate } = featureFilter(filter, 'filter');
  return Object.keys(WATERWAYS).filter(
    name => evaluate({ zoom: 14 }, { type: 'LineString', properties: WATERWAYS[name] }),
  ).sort();
}

describe('downstreamFilter', () => {
  it('matches the waterway and everything downstream of it', () => {
    expect(matching(downstreamFilter(WATERWAYS.e.flow_pre))).toEqual(['a', 'b', 'd', 'e']);
  });

  it('leaves out tributaries that join downstream', () => {
    expect(matching(downstreamFilter(WATERWAYS.c.flow_pre))).toEqual(['a', 'b', 'c']);
  });

  it('matches only the outlet when tracing from the outlet', () => {
    expect(matching(downstreamFilter(WATERWAYS.a.flow_pre))).toEqual(['a']);
  });

  it('leaves out other rivers', () => {
    expect(matching(downstreamFilter(WATERWAYS.y.flow_pre))).toEqual(['x', 'y']);
  });
});

describe('NO_TRACE_FILTER', () => {
  it('matches nothing', () => {
    expect(matching(NO_TRACE_FILTER)).toEqual([]);
  });
});

describe('flowPre', () => {
  it('reads the flow label from a waterway', () => {
    expect(flowPre({ sourceLayer: 'waterways', properties: { flow_pre: 3 } })).toBe(3);
  });

  it('is undefined for a waterway without flow labels', () => {
    // e.g. from a source without flow data, or a pack built before waterways had labels
    expect(flowPre({ sourceLayer: 'waterways', properties: { name: 'Temescal Creek' } }))
      .toBeUndefined();
  });

  it('is undefined for other water features', () => {
    expect(flowPre({ sourceLayer: 'waterbodies', properties: { flow_pre: 3 } }))
      .toBeUndefined();
  });
});
