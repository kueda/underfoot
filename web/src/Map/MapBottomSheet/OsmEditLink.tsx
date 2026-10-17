import type { Map } from 'maplibre-gl';

import type { UnderfootFeature } from '../../packs/types';

// The OSM editor where the map is. MapLibre's zoom levels are based on 512px
// tiles and OSM's on 256px tiles, so OSM shows the same area one level up.
function osmEditUrl(map: Map) {
  const { lat, lng } = map.getCenter();
  const zoom = Math.round(map.getZoom()) + 1;
  return `https://www.openstreetmap.org/edit#map=${zoom}/${lat.toFixed(5)}/${lng.toFixed(5)}`;
}

// Invites fixing features from OpenStreetMap there, since fixes show up in
// Underfoot when its packs are next built
export default function OsmEditLink({ feature, map }: {
  feature?: UnderfootFeature;
  map?: Map;
}) {
  if (feature?.source !== 'osm' || !map) return null;
  return (
    <p>
      <small>
        Anyone can edit OpenStreetMap, so you can <a href={osmEditUrl(map)} target="_blank" rel="noreferrer">edit this!</a>
      </small>
    </p>
  );
}
