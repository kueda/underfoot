import marshTuftsTemplate from './marsh-tufts.svg?raw';

// The SVG is drawn at twice its size in CSS pixels, so the tufts stay crisp
// on high density screens
export const MARSH_PATTERN_PIXEL_RATIO = 2;

// The marsh tufts SVG drawn in a color
export function marshTuftsSvg(color: string): string {
  return marshTuftsTemplate.replace(/currentColor/g, color);
}

// Loads a tile of marsh tufts in a color on a transparent background, ready
// to add to the map as a pattern
export async function loadMarshTufts(color: string): Promise<HTMLImageElement> {
  const image = new Image();
  image.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(marshTuftsSvg(color))}`;
  await image.decode();
  return image;
}
