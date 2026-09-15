import CssBaseline from '@mui/material/CssBaseline';
import React from 'react';

import AppBar from './AppBar';
import Map from './Map/Map';
import PacksDialog from './PacksDialog/PacksDialog';
import LogDialog from './LogDialog/LogDialog';
import AboutDialog from './AboutDialog/AboutDialog';

import './App.css';
import { usePackStore } from './packs/usePackStore';
import {
  ROCKS,
  WATER,
  useCurrentPackId,
  useMapType,
  useRequestedPackId,
  useRequestPack,
  useSetCurrentPackId,
  useSetMapType,
} from './useAppStore';
import { initialHashParams, setHashParam } from './urlHash';

function App() {
  const setCurrentPackId = useSetCurrentPackId();
  const currentPackId = useCurrentPackId();
  const setMapType = useSetMapType();
  const mapType = useMapType();
  const requestPack = useRequestPack();
  const requestedPackId = useRequestedPackId();
  const packStore = usePackStore();
  const [hashApplied, setHashApplied] = React.useState(false);
  const hashResolveStarted = React.useRef(false);

  // Resolve the initial map view from the URL hash (for shared/bookmarked
  // links), falling back to the last pack the user had open. Runs once.
  React.useEffect(() => {
    if (hashResolveStarted.current) return;
    hashResolveStarted.current = true;

    async function applyInitialHash() {
      if (initialHashParams.type === ROCKS || initialHashParams.type === WATER) {
        setMapType(initialHashParams.type);
      }
      const hashPackId = initialHashParams.pack;
      if (hashPackId) {
        const pack = await packStore.get(hashPackId);
        if (pack?.data) {
          setCurrentPackId(hashPackId);
          packStore.setCurrent(hashPackId);
        }
        else {
          // The linked pack isn't downloaded (or isn't known yet); prompt for it.
          // Map.tsx pans/zooms to the hash location once the pack finishes loading.
          requestPack(hashPackId);
        }
      }
      else {
        const lastPackId = await packStore.getCurrentPackId();
        if (lastPackId) setCurrentPackId(lastPackId);
      }
      setHashApplied(true);
    }

    applyInitialHash().catch(e => console.error('Failed to apply initial URL hash', e));
  }, [packStore, requestPack, setCurrentPackId, setMapType]);

  // Keep the hash in sync with the current view so it can be shared/bookmarked.
  React.useEffect(() => {
    if (!hashApplied) return;
    // While a linked pack is still waiting to be downloaded there's no current
    // pack yet, but the hash should keep pointing at it so a reload still works.
    if (!currentPackId && requestedPackId) return;
    setHashParam('pack', currentPackId);
  }, [hashApplied, currentPackId, requestedPackId]);

  React.useEffect(() => {
    if (!hashApplied) return;
    setHashParam('type', mapType);
  }, [hashApplied, mapType]);

  return (
    <>
      <CssBaseline />
      <AppBar />
      <Map />
      <PacksDialog />
      <LogDialog />
      <AboutDialog />
    </>
  );
}

export default App;
