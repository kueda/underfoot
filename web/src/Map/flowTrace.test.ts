import { featureFilter } from '@maplibre/maplibre-gl-style-spec';
import type { FilterSpecification } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';

import {
  downstreamFilter,
  flowLabels,
  NO_TRACE_FILTER,
  upstreamFilter,
} from './flowTrace';

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

function labelsOf(name: string) {
  return { pre: WATERWAYS[name].flow_pre, upstream: WATERWAYS[name].flow_upstream };
}

describe('downstreamFilter', () => {
  it('matches the waterway and everything downstream of it', () => {
    expect(matching(downstreamFilter(labelsOf('e')))).toEqual(['a', 'b', 'd', 'e']);
  });

  it('leaves out tributaries that join downstream', () => {
    expect(matching(downstreamFilter(labelsOf('c')))).toEqual(['a', 'b', 'c']);
  });

  it('matches only the outlet when tracing from the outlet', () => {
    expect(matching(downstreamFilter(labelsOf('a')))).toEqual(['a']);
  });

  it('leaves out other rivers', () => {
    expect(matching(downstreamFilter(labelsOf('y')))).toEqual(['x', 'y']);
  });
});

describe('upstreamFilter', () => {
  it('matches the waterway and every waterway that flows into it', () => {
    expect(matching(upstreamFilter(labelsOf('b')))).toEqual(['b', 'c', 'd', 'e']);
  });

  it('leaves out waterways downstream and tributaries of them', () => {
    expect(matching(upstreamFilter(labelsOf('d')))).toEqual(['d', 'e']);
  });

  it('matches only the headwater when tracing from a headwater', () => {
    expect(matching(upstreamFilter(labelsOf('c')))).toEqual(['c']);
  });

  it('leaves out other rivers', () => {
    expect(matching(upstreamFilter(labelsOf('a')))).toEqual(['a', 'b', 'c', 'd', 'e']);
  });
});

describe('NO_TRACE_FILTER', () => {
  it('matches nothing', () => {
    expect(matching(NO_TRACE_FILTER)).toEqual([]);
  });
});

describe('flowLabels', () => {
  it('reads the flow labels from a waterway', () => {
    expect(flowLabels({
      sourceLayer: 'waterways',
      properties: { flow_pre: 3, flow_upstream: 1 },
    })).toEqual({ pre: 3, upstream: 1 });
  });

  it('is undefined for a waterway without flow labels', () => {
    // e.g. from a source without flow data, or a pack built before waterways had labels
    expect(flowLabels({ sourceLayer: 'waterways', properties: { name: 'Temescal Creek' } }))
      .toBeUndefined();
  });

  it('is undefined for other water features', () => {
    expect(flowLabels({
      sourceLayer: 'waterbodies',
      properties: { flow_pre: 3, flow_upstream: 1 },
    })).toBeUndefined();
  });
});
