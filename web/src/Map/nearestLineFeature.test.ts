import { describe, expect, it } from 'vitest';

import { nearestLineFeature } from './nearestLineFeature';

// Treats coordinates as screen pixels, so distances are easy to reason about
const project = ([x, y]: number[]) => ({ x, y });
const point = { x: 0, y: 0 };

function line(name: string, ...coordinates: number[][]) {
  return { name, geometry: { type: 'LineString' as const, coordinates } };
}

describe('nearestLineFeature', () => {
  it('picks the line closest to the point', () => {
    const lines = [
      line('far', [-10, 8], [10, 8]),
      line('near', [-10, 3], [10, 3]),
    ];
    expect(nearestLineFeature(lines, point, project, 10)?.name).toBe('near');
  });

  it('measures to the nearest point along a line, not just its vertices', () => {
    const lines = [
      // Passes 2px from the point, but its vertices are 20px away
      line('long', [-20, 2], [20, 2]),
      line('short', [4, 4], [5, 5]),
    ];
    expect(nearestLineFeature(lines, point, project, 10)?.name).toBe('long');
  });

  it('measures to every part of a multi-line', () => {
    const multiLine = {
      name: 'multi',
      geometry: {
        type: 'MultiLineString' as const,
        coordinates: [[[-10, 9], [10, 9]], [[-10, 1], [10, 1]]],
      },
    };
    const lines = [line('single', [-10, 5], [10, 5]), multiLine];
    expect(nearestLineFeature(lines, point, project, 10)?.name).toBe('multi');
  });

  it('ignores lines farther away than the max distance', () => {
    expect(nearestLineFeature([line('far', [-20, 12], [20, 12])], point, project, 10))
      .toBeUndefined();
  });

  it('ignores features that are not lines', () => {
    const polygon = {
      name: 'polygon',
      geometry: {
        type: 'Polygon' as const,
        coordinates: [[[-1, -1], [1, -1], [1, 1], [-1, -1]]],
      },
    };
    expect(nearestLineFeature([polygon], point, project, 10)).toBeUndefined();
  });
});
