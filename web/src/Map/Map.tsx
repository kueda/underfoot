import { useCallback, useEffect, useRef, useState } from 'react';
import { addProtocol, Map, type MapGeoJSONFeature, ScaleControl, setWorkerUrl } from 'maplibre-gl';
import maplibreWorkerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';
import 'maplibre-gl/dist/maplibre-gl.css';
import * as pmtiles from 'pmtiles';
import Modal from '@mui/material/Modal';
import CircularProgress from '@mui/material/CircularProgress';
import Button from '@mui/material/Button';
import AddIcon from '@mui/icons-material/Add';

import { usePackStore } from '../packs/usePackStore';
import { UnderfootFeature, WaterFeature } from '../packs/types';
import { initialHashParams } from '../urlHash';
import {
  addLog,
  useCurrentPackId,
  useMapType,
  useSetCurrentPackId,
  useShowPacksModal,
  useLogging,
} from '../useAppStore';
import MapBottomSheet from './MapBottomSheet/MapBottomSheet';
import CurrentLocationButton from './CurrentLocationButton';
import DownstreamButton from './DownstreamButton';
import {
  DOWNSTREAM_LAYER_ID,
  NO_TRACE_FILTER,
  downstreamFilter,
  flowPre,
} from './flowTrace';
import { nearestLineFeature } from './nearestLineFeature';
import { Citations, UnderfootFeatures } from './types';
import { NO_STYLE } from './mapStyles';
import { loadMapFromPackData } from './util';

// Wrap native fetch to monitor all network requests and log failures
const originalFetch = window.fetch;
window.fetch = async (...args) => {
  const [resource] = args;
  const url = typeof resource === 'string'
    ? resource
    : resource instanceof Request
      ? resource.url
      : resource.href;
  const startTime = performance.now();

  try {
    const response = await originalFetch(...args);
    const elapsed = (performance.now() - startTime).toFixed(0);

    if (!response.ok) {
      const resourceType = url.includes('/font/')
        ? 'font'
        : url.includes('.pbf')
          ? 'tile'
          : url.includes('.json')
            ? 'metadata'
            : 'resource';
      addLog(`[Fetch] Failed to load ${resourceType}: ${url} (${response.status} ${response.statusText}, ${elapsed}ms)`);
    }

    return response;
  }
  catch (error) {
    const elapsed = (performance.now() - startTime).toFixed(0);
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    const resourceType = url.includes('/font/')
      ? 'font'
      : url.includes('.pbf')
        ? 'tile'
        : url.includes('.json')
          ? 'metadata'
          : 'resource';
    addLog(`[Fetch] Network error loading ${resourceType}: ${url} (${errorMessage}, ${elapsed}ms)`);
    throw error;
  }
};

// How many pixels from the crosshairs a waterway can be and still count as
// under them. The Android app used the same radius.
const CROSSHAIRS_WATERWAY_RADIUS = 10;

// MapLibre 6 can't find its worker script from inside a bundle, so point it at
// the worker chunk Vite builds from the ?worker&url import above.
setWorkerUrl(maplibreWorkerUrl);

// add the PMTiles plugin to MapLibre's global protocol registry.
const protocol = new pmtiles.Protocol();
addProtocol('pmtiles', request => {
  // Log tile requests for debugging
  const tileMatch = request.url.match(/pmtiles:\/\/(\w+)\/(\d+)\/(\d+)\/(\d+)/);
  const requestStart = performance.now();
  if (tileMatch) {
    const [, source, z, x, y] = tileMatch;
    addLog(`[PMTiles] Requesting tile ${source} ${z}/${x}/${y}`);
  }

  return new Promise((resolve, reject) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const callback = (err: Error | undefined, data: any) => {
      const elapsed = (performance.now() - requestStart).toFixed(0);
      if (err) {
        addLog(`[PMTiles] Tile fetch failed after ${elapsed}ms: ${err.message}`);
        reject(err);
      }
      else {
        if (tileMatch) {
          const [, source] = tileMatch;
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
          const dataSize = data?.byteLength || data?.length || 0;
          addLog(`[PMTiles] Tile fetch succeeded for ${source} (${dataSize} bytes, ${elapsed}ms)`);
        }
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        resolve({ data });
      }
    };
    protocol.tile(request, callback);
  });
});

export default function UnderfootMap() {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<Map>();
  const mapType = useMapType();
  const currentPackId = useCurrentPackId();
  const setCurrentPackId = useSetCurrentPackId();
  const showPacksModal = useShowPacksModal();
  const packStore = usePackStore();
  const [loadedPackId, setLoadedPackId] = useState<string | null>(null);
  const [loadedMapType, setLoadedMapType] = useState<string | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);
  const [packLoading, setPackLoading] = useState(false);
  // Synchronous guard against changePack running again before an in-flight load
  // finishes. The packLoading state lags a render behind, so effect re-runs
  // triggered while a load is starting (e.g. right after a download) could
  // otherwise kick off a second, concurrent load.
  const packLoadingRef = useRef(false);
  const [mapFeature, setMapFeature] = useState<MapGeoJSONFeature>();
  const [underfootFeature, setUnderfootFeature] = useState<UnderfootFeature>();
  const [underfootFeatures, setUnderfootFeatures] = useState<UnderfootFeatures>({});
  const [citations, setCitations] = useState<Citations>({});
  // flow_pre of the waterway a downstream trace starts from, if one is showing
  const [downstreamTracePre, setDownstreamTracePre] = useState<number | null>(null);
  const { add: log } = useLogging();
  // A location from the URL hash (shared link) that should override the default
  // "recenter on the pack" behavior the first time a pack loads. Consumed once.
  const pendingHashLocation = useRef(initialHashParams.location);

  // Update the "what's under the crosshairs" feature for the map's current
  // center. Called while panning and again once the map settles, since a shared
  // URL can position the map without any user move to trigger the lookup.
  const refreshCenterFeature = useCallback((type: string | null) => {
    const mapInstance = map.current;
    if (!mapInstance) return;
    const center = mapInstance.project(mapInstance.getCenter());
    // Waterways are too thin to put the crosshairs right on, so pick the
    // nearest one within a few pixels. The layer isn't there while the style
    // is switching.
    if (type !== 'rocks' && mapInstance.getLayer('waterways')) {
      const radius = CROSSHAIRS_WATERWAY_RADIUS;
      const nearbyWaterways = mapInstance.queryRenderedFeatures(
        [[center.x - radius, center.y - radius], [center.x + radius, center.y + radius]],
        { layers: ['waterways'] },
      );
      const waterway = nearestLineFeature(
        nearbyWaterways,
        center,
        ([lng, lat]) => mapInstance.project([lng, lat]),
        radius,
      );
      if (waterway) {
        setMapFeature(waterway);
        return;
      }
    }
    const features = mapInstance.queryRenderedFeatures(center);
    if (features.length === 0) {
      setMapFeature(undefined);
      return;
    }
    const feature = type === 'rocks'
      ? features.find(f => f.sourceLayer === 'rock_units')
      : features.find(f => f.sourceLayer === 'waterbodies')
        || features.find(f => f.sourceLayer === 'watersheds');
    setMapFeature(feature);
  }, []);

  useEffect(() => {
    if (!mapContainer.current) return;

    if (!map.current) {
      map.current = new Map({
        container: mapContainer.current,
        center: [-122, 38],
        zoom: 2,
        maxZoom: 22,
        attributionControl: false,
        // Sync zoom/lat/lng to the URL as `#map=<zoom>/<lat>/<lng>`. The named
        // form leaves our other hash params (pack, type) untouched.
        hash: 'map',
      });
      map.current.on('load', () => {
        setMapLoaded(true);
        log('Map initial load complete');
      });
      map.current.on('idle', () => {
        log(`Map idle - layers order: ${JSON.stringify(map.current?.getLayersOrder())}`);
      });
      map.current.on('sourcedata', e => {
        if (e.isSourceLoaded) {
          log(`Source ${e.sourceId} finished loading`);
        }
      });
      map.current.on('data', e => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
        const dataType = (e as any).dataType;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
        const sourceId = (e as any).sourceId;
        if (dataType === 'style') {
          log('[MapLibre] Style data loaded');
        }
        else if (dataType === 'source' && sourceId) {
          log(`[MapLibre] Source metadata loaded: ${sourceId}`);
        }
      });
      map.current.on('dataloading', e => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
        const dataType = (e as any).dataType;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
        const sourceId = (e as any).sourceId;
        if (dataType === 'style') {
          log('[MapLibre] Loading style data...');
        }
        else if (dataType === 'source' && sourceId) {
          log(`[MapLibre] Loading source metadata: ${sourceId}`);
        }
      });
      map.current.on('styleimagemissing', e => {
        log(`[MapLibre] Missing style image: ${e.id}`);
      });
      map.current.on('error', e => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
        const errorEvent = e as any;
        const error = e.error as Error | undefined;
        const errorMessage = error?.message || 'Unknown error';
        const errorType = error?.name || 'Error';
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
        const sourceId = errorEvent.sourceId || 'unknown source';

        let detailedMessage = `Map error [${errorType}]: ${errorMessage}`;
        if (sourceId !== 'unknown source') detailedMessage += ` (source: ${sourceId})`;

        // Try to extract tile info if available
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        if (errorEvent.tile?.tileID?.canonical) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
          const canonical = errorEvent.tile.tileID.canonical;
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          detailedMessage += ` (tile ${canonical.z}/${canonical.x}/${canonical.y})`;
        }

        log(detailedMessage);
        console.error('[Map] Error event:', e);
      });
      map.current.on('click', clickEvent => {
        map.current?.panTo(clickEvent.lngLat);
      });
      const scale = new ScaleControl({
        maxWidth: 80,
        unit: 'metric',
      });
      map.current.addControl(scale, 'bottom-left');
    }

    map.current.on('move', () => {
      refreshCenterFeature(loadedMapType);
    });
  }, [loadedMapType, log, map, mapContainer, refreshCenterFeature]);

  // The map is often stationary right after a pack or map-type load (especially
  // when a shared URL positioned it), so "move" never fires to populate the
  // bottom sheet. Re-query the crosshairs feature each time the map settles.
  useEffect(() => {
    const mapInstance = map.current;
    if (!mapInstance || !loadedMapType) return;
    const handleIdle = () => refreshCenterFeature(loadedMapType);
    mapInstance.on('idle', handleIdle);
    return () => {
      mapInstance.off('idle', handleIdle);
    };
  }, [loadedPackId, loadedMapType, refreshCenterFeature]);

  useEffect(() => {
    if (!loadedMapType) return;
    if (!mapFeature) {
      setUnderfootFeature(undefined);
      return;
    }
    if (loadedMapType === 'rocks') {
      if (underfootFeatures && mapFeature.properties.id) {
        const feature = underfootFeatures[parseInt(String(mapFeature.properties.id), 10)];
        if (citations && feature?.source && !feature.citation) {
          feature.citation = citations[feature.source];
        }
        setUnderfootFeature(feature);
      }
      else {
        setUnderfootFeature(undefined);
      }
    }
    else {
      const newUnderfootFeature: WaterFeature = {
        source: String(mapFeature.properties.source),
        layer: String(mapFeature.sourceLayer),
      };
      if (mapFeature.properties.name) newUnderfootFeature.title = mapFeature.properties.name as string;
      if (citations && newUnderfootFeature.source) {
        newUnderfootFeature.citation = citations[newUnderfootFeature.source];
      }
      setUnderfootFeature(newUnderfootFeature);
    }
  }, [
    citations,
    loadedMapType,
    mapFeature,
    underfootFeatures,
  ]);

  useEffect(() => {
    async function changePack() {
      log('changePack');
      if (currentPackId === loadedPackId && mapType === loadedMapType) return;
      if (!map.current) return;
      if (packLoadingRef.current) return;
      packLoadingRef.current = true;
      setPackLoading(true);
      // A new style replaces the trace layer's filter, and a trace from another
      // pack's labels wouldn't mean anything anyway
      setDownstreamTracePre(null);
      // If there's no pack, ensure style gets reset so map is blank
      if (!currentPackId) {
        setLoadedPackId(null);
        map.current.setStyle(NO_STYLE);
        packLoadingRef.current = false;
        setPackLoading(false);
        return;
      }
      const currentPack = await packStore.get(currentPackId);
      if (!currentPack?.data) throw new Error(`Pack not downloaded: ${currentPackId}`);
      let packData;
      try {
        packData = await currentPack.unzippedData();
      }
      catch (e) {
        const unzipError = e as Error;
        log(`changePack failed to unzip: ${unzipError.message}`);
        throw unzipError;
      }

      // Load ways
      if (!packData.ways_pmtiles) throw new Error(`Pack ${currentPackId} did not have ways data`);
      let waysPmtiles;
      try {
        waysPmtiles = new pmtiles.PMTiles(
          new pmtiles.FileSource(
            // The filename is important b/c it's a key that we use to refer to
            // this "protocol" in the sources
            new File([packData.ways_pmtiles], 'ways'),
          ),
        );
        protocol.add(waysPmtiles);
        // Validate that PMTiles is actually accessible
        await waysPmtiles.getHeader();
        log(`Successfully loaded ways PMTiles for pack ${currentPackId}`);
      }
      catch (waysError) {
        const error = waysError as Error;
        log(`Failed to load ways PMTiles: ${error.message}`);
        throw new Error(`Failed to load ways data for pack ${currentPackId}: ${error.message}`);
      }

      // Load contours
      if (!packData.contours_pmtiles) throw new Error(`Pack ${currentPackId} did not have contours data`);
      let contoursPmtiles;
      try {
        contoursPmtiles = new pmtiles.PMTiles(
          new pmtiles.FileSource(
            // The filename is important b/c it's a key that we use to refer to
            // this "protocol" in the sources
            new File([packData.contours_pmtiles], 'contours'),
          ),
        );
        protocol.add(contoursPmtiles);
        // Validate that PMTiles is actually accessible
        await contoursPmtiles.getHeader();
        log(`Successfully loaded contours PMTiles for pack ${currentPackId}`);
      }
      catch (contoursError) {
        const error = contoursError as Error;
        log(`Failed to load contours PMTiles: ${error.message}`);
        throw new Error(`Failed to load contours data for pack ${currentPackId}: ${error.message}`);
      }

      // Load context
      if (!packData.context_pmtiles) throw new Error(`Pack ${currentPackId} did not have context data`);
      let contextPmtiles;
      try {
        contextPmtiles = new pmtiles.PMTiles(
          new pmtiles.FileSource(
            // The filename is important b/c it's a key that we use to refer to
            // this "protocol" in the sources
            new File([packData.context_pmtiles], 'context'),
          ),
        );
        protocol.add(contextPmtiles);
        // Validate that PMTiles is actually accessible
        await contextPmtiles.getHeader();
        log(`Successfully loaded context PMTiles for pack ${currentPackId}`);
      }
      catch (contextError) {
        const error = contextError as Error;
        log(`Failed to load context PMTiles: ${error.message}`);
        throw new Error(`Failed to load context data for pack ${currentPackId}: ${error.message}`);
      }

      log(`Calling loadMapFromPackData for ${mapType}`);
      loadMapFromPackData(
        packData,
        protocol,
        map.current,
        mapType,
        setUnderfootFeatures,
        setCitations,
      );

      // Track style loading completion
      void map.current.once('styledata', () => {
        log(`Map styledata event fired for pack ${currentPackId}`);
      });

      void map.current.once('style.load', () => {
        log(`Map style.load event fired for pack ${currentPackId}`);
      });

      // Only reset the camera when switching to a different pack. Switching
      // mapType (e.g. rocks <-> water) on the same pack should preserve the
      // user's current view.
      if (currentPackId !== loadedPackId) {
        if (pendingHashLocation.current) {
          // A shared link specified a location; honor it instead of recentering
          // on the pack. Only applies to the first pack load.
          const { zoom, lat, lng } = pendingHashLocation.current;
          map.current.jumpTo({ center: [lng, lat], zoom });
          pendingHashLocation.current = null;
        }
        else {
          const waysHeader = await waysPmtiles.getHeader();
          map.current.setZoom(waysHeader.maxZoom - 2);
          map.current.setCenter([waysHeader.centerLon, waysHeader.centerLat]);
        }
      }
      setLoadedPackId(currentPackId);
      setLoadedMapType(mapType);
      packLoadingRef.current = false;
      setPackLoading(false);
    }
    if (
      packStore
      && (currentPackId !== loadedPackId || mapType !== loadedMapType)
      && map.current
    ) {
      changePack().catch(e => {
        const error = e as Error;
        // Reset loading state and the pack selection so a failed load falls
        // back to the "no pack selected" screen instead of retrying forever
        // with the same broken pack (and re-throwing on every retry).
        packLoadingRef.current = false;
        setPackLoading(false);
        setCurrentPackId(null);
        packStore.setCurrent(null);
        alert(`Failed to change to pack ${currentPackId}: ${error.message}`);
        log(error.message);
        console.error(`Failed to change to pack ${currentPackId}`, error);
      });
    }
  }, [
    log,
    currentPackId,
    loadedMapType,
    loadedPackId,
    mapLoaded,
    mapType,
    packStore,
    setCurrentPackId,
  ]);

  const crosshairFlowPre = mapFeature ? flowPre(mapFeature) : undefined;

  // Traces downstream from the waterway under the crosshairs, or clears the
  // trace if one is showing
  function toggleDownstreamTrace() {
    const pre = downstreamTracePre === null ? crosshairFlowPre ?? null : null;
    map.current?.setFilter(
      DOWNSTREAM_LAYER_ID,
      pre === null ? NO_TRACE_FILTER : downstreamFilter(pre),
    );
    setDownstreamTracePre(pre);
  }

  return (
    <div className="map-wrapper">
      <div className={`map ${loadedPackId ? 'loaded' : ''}`} ref={mapContainer} />
      <CurrentLocationButton map={map.current} />
      { loadedPackId && (
        <>
          <AddIcon fontSize="large" className="add-icon" style={{ pointerEvents: 'none' }} />
          { loadedMapType === 'water' && (
            <DownstreamButton
              traceable={crosshairFlowPre !== undefined}
              active={downstreamTracePre !== null}
              onClick={toggleDownstreamTrace}
            />
          )}
          <MapBottomSheet feature={underfootFeature} mapType={mapType} />
        </>
      ) }
      { !loadedPackId && !packLoading && (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            maxWidth: '50%',
            textAlign: 'center',
          }}
        >
          <p>
            Welcome to Underfoot, an offline geologic and hydrologic map!
            <br />
            To get started, download some data to use offline.
          </p>
          <Button onClick={showPacksModal} variant="contained">DOWNLOAD MAP DATA</Button>
        </div>
      )}
      <Modal
        open={packLoading}
        className="loading-modal"
      >
        <div className="inner">
          <CircularProgress />
        </div>
      </Modal>
    </div>
  );
}
