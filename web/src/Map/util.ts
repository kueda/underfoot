import type { Map, StyleSpecification, VectorSourceSpecification } from 'maplibre-gl';
import Papa from 'papaparse';
import * as pmtiles from 'pmtiles';

import type { Pack } from '../packs/Pack';
import { RockUnit, UnzippedPackData } from '../packs/types';
import { Citation, Citations, UnderfootFeatures } from './types';
import { ROCK_STYLE, WATER_STYLE } from './mapStyles';

// Where the map starts when a pack loads: the pack's default view if it has
// one, otherwise the center of its ways tiles, zoomed out a bit from their
// maxzoom
export function packStartView(
  pack: Pick<Pack, 'defaultCenter' | 'defaultZoom'>,
  waysHeader: Pick<pmtiles.Header, 'centerLat' | 'centerLon' | 'maxZoom'>,
): { center: [number, number]; zoom: number } {
  const center = pack.defaultCenter || {
    lat: waysHeader.centerLat,
    lon: waysHeader.centerLon,
  };
  return {
    center: [center.lon, center.lat],
    zoom: pack.defaultZoom ?? waysHeader.maxZoom - 2,
  };
}

export async function loadMapFromPackData(
  packData: UnzippedPackData,
  protocol: pmtiles.Protocol,
  map: Map,
  mapType: string,
  setFeatures: React.Dispatch<React.SetStateAction<UnderfootFeatures>>,
  setCitations: React.Dispatch<React.SetStateAction<Citations>>,
  contoursMaxZoom: number,
) {
  let pmtilesBlob: Blob | undefined;
  let featuresBlob: Blob | undefined;
  let citationsBlob: Blob | undefined;
  let style: StyleSpecification;
  if (mapType === 'rocks') {
    pmtilesBlob = packData.rocks_pmtiles;
    featuresBlob = packData.rocks_units_csv;
    citationsBlob = packData.rocks_citations_csv;
    style = ROCK_STYLE;
  }
  else {
    pmtilesBlob = packData.water_pmtiles;
    citationsBlob = packData.water_citations_csv;
    style = WATER_STYLE;
  }
  if (!pmtilesBlob) throw new Error(`Pack did not have ${mapType} data`);
  if (!citationsBlob) throw new Error(`Pack did not have ${mapType} citations`);

  const pmtilesData = new pmtiles.PMTiles(
    new pmtiles.FileSource(new File([pmtilesBlob], mapType)),
  );
  protocol.add(pmtilesData);
  // Packs from small-scale sources may stop short of the style's maxzoom, so
  // overzoom past whatever their tiles reach. Contours stop at the zoom of
  // the elevation tiles they came from.
  const { maxZoom } = await pmtilesData.getHeader();
  const source = style.sources[mapType] as VectorSourceSpecification;
  const contoursSource = style.sources.contours as VectorSourceSpecification;
  map.setStyle({
    ...style,
    sources: {
      ...style.sources,
      [mapType]: { ...source, maxzoom: maxZoom },
      contours: { ...contoursSource, maxzoom: contoursMaxZoom },
    },
  });
  if (featuresBlob) {
    Papa.parse(new File([featuresBlob], `${mapType}_metadata`), {
      header: true,
      dynamicTyping: true,
      complete: results => {
        const emptyFeatures: UnderfootFeatures = {};
        const newFeatures = results.data.reduce((memo, curr) => {
          const feature = curr as RockUnit;
          const featuresMemo: UnderfootFeatures = memo as UnderfootFeatures;
          featuresMemo[feature.id] = feature;
          return featuresMemo;
        }, emptyFeatures);
        setFeatures(newFeatures as UnderfootFeatures);
      },
    });
  }
  if (citationsBlob) {
    Papa.parse(new File([citationsBlob], `${mapType}_citations`), {
      header: true,
      dynamicTyping: true,
      complete: results => {
        const emptyCitations: Citations = {};
        const newCitations = results.data.reduce((memo, curr) => {
          const citation = curr as Citation;
          const citationsMemo: Citations = memo as Citations;
          citationsMemo[citation.source] = citation.citation;
          return citationsMemo;
        }, emptyCitations) as Citations;
        setCitations((existing: Citations) => ({ ...existing, ...newCitations }));
      },
    });
  }
}
