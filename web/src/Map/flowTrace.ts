import type { ExpressionFilterSpecification, MapGeoJSONFeature } from 'maplibre-gl';

// Tracing where water flows relies on two labels data/water.py puts on
// waterways: flow_pre, the waterway's place in a depth-first walk up its river
// from the outlet, and flow_upstream, how many waterways flow into it. That
// makes every waterway downstream of X one whose range of numbers,
// flow_pre to flow_pre + flow_upstream, includes X's flow_pre.

export const DOWNSTREAM_LAYER_ID = 'waterways-downstream';

// Red from the same ColorBrewer palette as the water blue and the orange of
// artificial waterways in mapStyles.ts
export const DOWNSTREAM_COLOR = '#E31A1C';

export const NO_TRACE_FILTER: ExpressionFilterSpecification = false;

export function downstreamFilter(pre: number): ExpressionFilterSpecification {
  return [
    'all',
    // Waterways from sources without flow data don't have labels
    ['has', 'flow_upstream'],
    ['<=', ['get', 'flow_pre'], pre],
    ['>=', ['+', ['get', 'flow_pre'], ['get', 'flow_upstream']], pre],
  ];
}

// The flow label of a waterway on the map, if it has one
export function flowPre(
  feature: Pick<MapGeoJSONFeature, 'sourceLayer' | 'properties'>,
): number | undefined {
  if (feature.sourceLayer !== 'waterways') return undefined;
  const pre: unknown = feature.properties.flow_pre;
  return typeof pre === 'number' ? pre : undefined;
}
