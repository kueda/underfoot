import { describe, expect, it } from 'vitest';

import { patternSvg } from './pattern';

describe('patternSvg', () => {
  it('draws marsh tufts in the color the map asks for', () => {
    const svg = patternSvg('marsh-tufts', 'rgb(1,2,3)');
    expect(svg).toContain('stroke="rgb(1,2,3)"');
    expect(svg).not.toContain('currentColor');
  });

  it('draws ice dots in the color the map asks for', () => {
    const svg = patternSvg('ice-dots', 'rgb(1,2,3)');
    expect(svg).toContain('<circle');
    expect(svg).toContain('fill="rgb(1,2,3)"');
    expect(svg).not.toContain('currentColor');
  });
});
