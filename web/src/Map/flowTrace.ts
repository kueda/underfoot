import type { ExpressionFilterSpecification, MapGeoJSONFeature } from 'maplibre-gl';

// Tracing where water flows relies on two labels data/water.py puts on
// waterways: flow_pre, the waterway's place in a depth-first walk up its river
// from the outlet, and flow_upstream, how many waterways flow into it. That
// gives each waterway a range of numbers, flow_pre to flow_pre + flow_upstream,
// that covers the waterway and everything upstream of it.

export type TraceDirection = 'downstream' | 'upstream';

export const TRACE_DIRECTIONS: TraceDirection[] = ['downstream', 'upstream'];

// Layers that draw each trace, which all get the trace's filter
export const TRACE_LAYER_IDS: Record<TraceDirection, { line: string; arrows: string }> = {
  downstream: { line: 'waterways-downstream', arrows: 'waterways-downstream-arrows' },
  upstream: { line: 'waterways-upstream', arrows: 'waterways-upstream-arrows' },
};

// Magenta and green stay distinct with red-green color blindness, which turns
// magenta blue and green khaki. Pinks with less blue than this turn grey, and
// brighter greens fade into the land. Since magenta can look like waterway
// blue, the rest of the water fades while a trace is showing.
export const TRACE_COLORS: Record<TraceDirection, string> = {
  downstream: '#E600E6',
  upstream: '#009E3A',
};

export interface FlowLabels {
  pre: number;
  upstream: number;
}

export const NO_TRACE_FILTER: ExpressionFilterSpecification = false;

// Waterways whose range includes the starting waterway's flow_pre
export function downstreamFilter({ pre }: FlowLabels): ExpressionFilterSpecification {
  return [
    'all',
    // Waterways from sources without flow data don't have labels
    ['has', 'flow_upstream'],
    ['<=', ['get', 'flow_pre'], pre],
    ['>=', ['+', ['get', 'flow_pre'], ['get', 'flow_upstream']], pre],
  ];
}

// Waterways whose flow_pre is in the starting waterway's range
export function upstreamFilter({ pre, upstream }: FlowLabels): ExpressionFilterSpecification {
  return [
    'all',
    // Waterways from sources without flow data don't have labels
    ['has', 'flow_pre'],
    ['>=', ['get', 'flow_pre'], pre],
    ['<=', ['get', 'flow_pre'], pre + upstream],
  ];
}

export const TRACE_FILTERS: Record<
  TraceDirection,
  (labels: FlowLabels) => ExpressionFilterSpecification
> = {
  downstream: downstreamFilter,
  upstream: upstreamFilter,
};

// The flow labels of a waterway on the map, if it has them
export function flowLabels(
  feature: Pick<MapGeoJSONFeature, 'sourceLayer' | 'properties'>,
): FlowLabels | undefined {
  if (feature.sourceLayer !== 'waterways') return undefined;
  const { flow_pre: pre, flow_upstream: upstream } = feature.properties as Record<string, unknown>;
  return typeof pre === 'number' && typeof upstream === 'number' ? { pre, upstream } : undefined;
}
