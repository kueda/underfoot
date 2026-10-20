import iceDotsTemplate from './ice-dots.svg?raw';
import marshTuftsTemplate from './marsh-tufts.svg?raw';

// The SVGs are drawn at twice their size in CSS pixels, so patterns stay
// crisp on high density screens
export const PATTERN_PIXEL_RATIO = 2;

const TEMPLATES = {
  'ice-dots': iceDotsTemplate,
  'marsh-tufts': marshTuftsTemplate,
};

export type PatternShape = keyof typeof TEMPLATES;

// A pattern's SVG drawn in a color
export function patternSvg(shape: PatternShape, color: string): string {
  return TEMPLATES[shape].replace(/currentColor/g, color);
}

// Loads a tile of a pattern in a color on a transparent background, ready to
// add to the map
export async function loadPattern(shape: PatternShape, color: string): Promise<HTMLImageElement> {
  const image = new Image();
  image.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(patternSvg(shape, color))}`;
  await image.decode();
  return image;
}
