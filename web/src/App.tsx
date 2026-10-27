import CssBaseline from '@mui/material/CssBaseline';
import React from 'react';

import AppBar from './AppBar';
import Map from './Map/Map';
import PacksDialog from './PacksDialog/PacksDialog';
import LogDialog from './LogDialog/LogDialog';
import AboutDialog from './AboutDialog/AboutDialog';
import InstallDialog from './InstallDialog/InstallDialog';
import { useInstallListeners } from './InstallDialog/useInstall';

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
import { initialHashParams, readHashParams, setHashParam } from './urlHash';

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
  useInstallListeners();

  // Show a pack named in the URL hash, or prompt to download it if it isn't
  // downloaded. Map.tsx pans/zooms to the hash location once the pack loads.
  const applyHashPack = React.useCallback(async (hashPackId: string) => {
    const pack = await packStore.get(hashPackId);
    if (pack?.isDownloaded()) {
      setCurrentPackId(hashPackId);
      packStore.setCurrent(hashPackId);
    }
    else {
      // The linked pack isn't downloaded (or isn't known yet); prompt for it.
      requestPack(hashPackId);
    }
  }, [packStore, requestPack, setCurrentPackId]);

  // Resolve the initial map view from the URL hash (for shared/bookmarked
  // links), falling back to the last pack the user had open. Runs once.
  React.useEffect(() => {
    if (hashResolveStarted.current) return;
    hashResolveStarted.current = true;

    async function applyInitialHash() {
      if (initialHashParams.type === ROCKS || initialHashParams.type === WATER) {
        setMapType(initialHashParams.type);
      }
      if (initialHashParams.pack) {
        await applyHashPack(initialHashParams.pack);
      }
      else {
        const lastPackId = await packStore.getCurrentPackId();
        if (lastPackId) setCurrentPackId(lastPackId);
      }
      setHashApplied(true);
    }

    applyInitialHash().catch(e => console.error('Failed to apply initial URL hash', e));
  }, [applyHashPack, packStore, setCurrentPackId, setMapType]);

  // Follow changes to the hash after the page has loaded, e.g. when a shared
  // link is pasted into a window that already has the app open, which changes
  // the hash without reloading the page. Our own hash updates use
  // history.replaceState, which doesn't fire hashchange.
  React.useEffect(() => {
    function onHashChange() {
      const { pack, type } = readHashParams();
      if (type === ROCKS || type === WATER) setMapType(type);
      if (pack) {
        applyHashPack(pack).catch(e => console.error('Failed to apply URL hash', e));
      }
    }
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, [applyHashPack, setMapType]);

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
      <InstallDialog />
    </>
  );
}

export default App;
