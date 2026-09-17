// Helpers for reading and writing the URL hash.
//
// MapLibre owns the `map=<zoom>/<lat>/<lng>` part of the hash when the map is
// created with `hash: 'map'`, and it leaves any other `&`-separated parameters
// intact. We use that to stash the current pack id and map type in the hash so a
// URL identifies a specific map view that can be shared or bookmarked.

export interface HashMapLocation {
  zoom: number;
  lat: number;
  lng: number;
}

function currentHashParts(): string[] {
  return window.location.hash.replace(/^#/, '').split('&').filter(Boolean);
}

export function getHashParam(key: string): string | null {
  for (const part of currentHashParts()) {
    const eq = part.indexOf('=');
    const partKey = eq === -1 ? part : part.slice(0, eq);
    if (partKey === key) {
      const value = eq === -1 ? '' : part.slice(eq + 1);
      return decodeURIComponent(value);
    }
  }
  return null;
}

// Sets or (when value is null/undefined) removes a single hash parameter,
// preserving every other parameter, including MapLibre's `map` location.
export function setHashParam(key: string, value: string | null | undefined): void {
  const kept = currentHashParts().filter(part => {
    const eq = part.indexOf('=');
    return (eq === -1 ? part : part.slice(0, eq)) !== key;
  });
  if (value !== null && value !== undefined) {
    kept.push(`${key}=${encodeURIComponent(value)}`);
  }
  const newHash = kept.length > 0 ? `#${kept.join('&')}` : '';
  const url = window.location.href.replace(/#.*$/, '') + newHash;
  window.history.replaceState(window.history.state, '', url);
}

export function parseHashMapLocation(raw: string | null): HashMapLocation | null {
  if (!raw) return null;
  const [zoom, lat, lng] = raw.split('/').map(Number);
  if ([zoom, lat, lng].some(n => !Number.isFinite(n))) return null;
  return { zoom, lat, lng };
}

// The hash as it was when the page first loaded, captured before any part of the
// app (including MapLibre) has had a chance to rewrite it.
export const initialHashParams = {
  location: parseHashMapLocation(getHashParam('map')),
  pack: getHashParam('pack'),
  type: getHashParam('type'),
};
