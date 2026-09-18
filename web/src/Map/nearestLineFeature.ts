import type { MapGeoJSONFeature } from 'maplibre-gl';

interface ScreenPoint {
  x: number;
  y: number;
}

// Distance from p to the closest spot on the segment from a to b
function distanceToSegment(p: ScreenPoint, a: ScreenPoint, b: ScreenPoint) {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const lengthSquared = dx * dx + dy * dy;
  // How far along the segment that spot is, from 0 at a to 1 at b
  const along = lengthSquared === 0
    ? 0
    : Math.max(0, Math.min(1, ((p.x - a.x) * dx + (p.y - a.y) * dy) / lengthSquared));
  return Math.hypot(p.x - (a.x + along * dx), p.y - (a.y + along * dy));
}

function linesOf(geometry: GeoJSON.Geometry): GeoJSON.Position[][] {
  if (geometry.type === 'LineString') return [geometry.coordinates];
  if (geometry.type === 'MultiLineString') return geometry.coordinates;
  return [];
}

// The line feature closest to a point on the screen, if any are within
// maxDistance pixels of it. project converts a feature's coordinates to
// screen pixels.
export function nearestLineFeature<F extends Pick<MapGeoJSONFeature, 'geometry'>>(
  features: F[],
  point: ScreenPoint,
  project: (position: GeoJSON.Position) => ScreenPoint,
  maxDistance: number,
): F | undefined {
  let nearest: F | undefined;
  let nearestDistance = maxDistance;
  for (const feature of features) {
    for (const line of linesOf(feature.geometry)) {
      const screenLine = line.map(project);
      for (let i = 1; i < screenLine.length; i += 1) {
        const distance = distanceToSegment(point, screenLine[i - 1], screenLine[i]);
        if (distance <= nearestDistance) {
          nearest = feature;
          nearestDistance = distance;
        }
      }
    }
  }
  return nearest;
}
