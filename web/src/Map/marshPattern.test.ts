import { describe, expect, it } from 'vitest';

import { marshTuftsSvg } from './marshPattern';

describe('marshTuftsSvg', () => {
  it('draws the tufts in the color the map asks for', () => {
    const svg = marshTuftsSvg('rgb(1,2,3)');
    expect(svg).toContain('stroke="rgb(1,2,3)"');
    expect(svg).not.toContain('currentColor');
  });
});
