import Box from '@mui/material/Box';
import ClearIcon from '@mui/icons-material/Clear';
import CircularProgress from '@mui/material/CircularProgress';
import CloseIcon from '@mui/icons-material/Close';
import MyLocationIcon from '@mui/icons-material/MyLocation';
import SearchIcon from '@mui/icons-material/Search';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import Tab from '@mui/material/Tab';
import TabContext from '@mui/lab/TabContext';
import TabList from '@mui/lab/TabList';
import TextField from '@mui/material/TextField';
import ToggleButton from '@mui/material/ToggleButton';
import Toolbar from '@mui/material/Toolbar';
import {
  useCallback, useEffect, useMemo, useRef, useState,
} from 'react';
import { Pack } from '../packs/Pack';
import { buildPackTree } from '../packs/packTree';
import { PackCenter } from '../packs/types';
import {
  useCurrentPackId,
  useHidePacksModal,
  useRequestedPackId,
  useSetCurrentPackId,
} from '../useAppStore';
import { usePackStore } from '../packs/usePackStore';
import AddLocalPackButton from './AddLocalPackButton';
import PackNodeList from './PackNodeList';
import PackTab from './PackTab';

// Packs whose names contain the search text, ignoring case. All of them when there's no text.
const filterPacks = (packs: Pack[] | null, query: string) => {
  const needle = query.trim().toLowerCase();
  if (!packs || !needle) return packs;
  return packs.filter(pack => pack.name.toLowerCase().includes(needle));
};

// Packs whose bounding boxes contain the location, the smallest first, since a small box is more
// likely to be the place you're in than a neighbor whose big box happens to overlap it. All of
// them when there's no location.
const filterPacksCovering = (packs: Pack[] | null, location: PackCenter | null) => {
  if (!packs || !location) return packs;
  const area = ({ bbox }: Pack) => (bbox.right - bbox.left) * (bbox.top - bbox.bottom);
  return packs
    .filter(({ bbox }) => (
      location.lon >= bbox.left
      && location.lon <= bbox.right
      && location.lat >= bbox.bottom
      && location.lat <= bbox.top
    ))
    .sort((a, b) => area(a) - area(b));
};

const filterPacksMatching = (
  packs: Pack[] | null,
  query: string,
  location: PackCenter | null,
) => filterPacksCovering(filterPacks(packs, query), location);

export default function Packs() {
  const packStore = usePackStore();
  const currentPackId = useCurrentPackId();
  const requestedPackId = useRequestedPackId();
  const onChoose = useSetCurrentPackId();
  const onClose = useHidePacksModal();
  const { list: listPacks, manifest, listLocal, error } = packStore;
  const [loading, setLoading] = useState(false);
  const [loadingLocal, setLoadingLocal] = useState(false);

  const [packs, setPacks] = useState<Pack[] | null>(null);
  const [downloadedPacks, setDownloadedPacks] = useState<Pack[] | null>(null);
  const [currentTab, setCurrentTab] = useState<'all' | 'downloaded'>('all');
  // The node of the pack hierarchy the "All" tab is showing the contents of; null is the top
  const [browseNodeId, setBrowseNodeId] = useState<string | null>(null);
  const [searchText, setSearchText] = useState('');
  const isSearching = searchText.trim() !== '';
  // Whether "Near me" is on. The lists only filter by location once there is one.
  const [nearMe, setNearMe] = useState(false);
  const [location, setLocation] = useState<PackCenter | null>(null);
  const [locationError, setLocationError] = useState<string | null>(null);
  const nearLocation = nearMe ? location : null;
  const isLocating = nearMe && !location;
  const isFiltering = isSearching || !!nearLocation;
  const packTree = useMemo(() => (packs ? buildPackTree(packs) : null), [packs]);

  const matchingPacks = useMemo(
    () => filterPacksMatching(packs, searchText, nearLocation),
    [packs, searchText, nearLocation],
  );
  const matchingDownloadedPacks = useMemo(
    () => filterPacksMatching(downloadedPacks, searchText, nearLocation),
    [downloadedPacks, searchText, nearLocation],
  );
  const noMatchesDescription = isSearching
    ? `No packs ${nearLocation ? 'near you ' : ''}match "${searchText.trim()}".`
    : 'No packs cover your location.';

  // Asks for the location every time it's turned on, since you may have moved since last time
  const toggleNearMe = () => {
    setLocationError(null);
    setLocation(null);
    if (nearMe) {
      setNearMe(false);
      return;
    }
    if (!navigator.geolocation) {
      setLocationError('Your browser can\'t share your location.');
      return;
    }
    setNearMe(true);
    navigator.geolocation.getCurrentPosition(
      position => setLocation({
        lat: position.coords.latitude,
        lon: position.coords.longitude,
      }),
      error => {
        setNearMe(false);
        setLocationError(
          error.code === error.PERMISSION_DENIED
            ? 'Couldn\'t get your location because permission was denied.'
            : 'Couldn\'t get your location.',
        );
      },
      // A rough location from a few minutes ago is enough to pick packs
      { maximumAge: 5 * 60 * 1000, timeout: 30 * 1000 },
    );
  };

  const packsLoaded = packs !== null;
  const downloadedPacksLoaded = downloadedPacks !== null;

  useEffect(() => {
    async function getPacks() {
      const listedPacks = await listPacks();
      setPacks(listedPacks);
    }
    if (manifest && !packsLoaded) {
      setLoading(true);
      getPacks()
        .catch(e => console.error('Failed to get packs', e))
        .finally(() => setLoading(false));
    }
  }, [
    listPacks,
    manifest,
    packsLoaded,
  ]);

  useEffect(() => {
    if (!downloadedPacksLoaded) {
      setLoadingLocal(true);
      listLocal()
        .then(localPacks => setDownloadedPacks(localPacks))
        .catch(e => console.error('Failed to get local packs', e))
        .finally(() => setLoadingLocal(false));
    }
  }, [listLocal, downloadedPacksLoaded]);

  // Re-reads both lists without emptying them first. Emptying them unmounts the rows, and a
  // row that's still downloading a pack would lose its progress and its stop button.
  const refreshPacks = useCallback(async () => {
    try {
      const [listedPacks, localPacks] = await Promise.all([listPacks(), listLocal()]);
      setPacks(listedPacks);
      setDownloadedPacks(localPacks);
    }
    catch (e) {
      console.error('Failed to refresh packs', e);
    }
  }, [listLocal, listPacks]);

  const isOffline = !!error?.message?.match(/NetworkError/);

  const requestedPack = requestedPackId
    ? (packs ?? []).concat(downloadedPacks ?? []).find(pack => pack.id === requestedPackId)
    : undefined;
  // True while a shared link points at a pack that still needs downloading. The
  // "All" tab is the only one that lists it, and PackListItem scrolls it into
  // view and points a prompt at its download button.
  const showRequestedPackPrompt = !!requestedPackId && !requestedPack?.isDownloaded();

  useEffect(() => {
    if (showRequestedPackPrompt) setCurrentTab('all');
  }, [showRequestedPackPrompt]);

  // Open the level the linked pack is on once the packs are listed, but only once per link so a
  // refresh of the lists doesn't undo where the user has browsed to since
  const openedRequestedPackId = useRef<string | null>(null);
  useEffect(() => {
    if (!showRequestedPackPrompt) {
      openedRequestedPackId.current = null;
      return;
    }
    if (!requestedPackId || !packTree || openedRequestedPackId.current === requestedPackId) return;
    openedRequestedPackId.current = requestedPackId;
    // A pack with packs inside it has no download button on its own row, so open the level
    // inside it, where its row does
    const requestedNode = packTree.get(requestedPackId);
    setBrowseNodeId(
      requestedNode?.children.length ? requestedNode.id : requestedNode?.parentId ?? null,
    );
  }, [packTree, requestedPackId, showRequestedPackPrompt]);

  useEffect(() => {
    // Once the linked pack is downloaded and active, the user is done here.
    if (requestedPackId && currentPackId === requestedPackId) onClose();
  }, [requestedPackId, currentPackId, onClose]);

  return (
    <>
      <Toolbar>
        <IconButton
          edge="start"
          color="inherit"
          onClick={onClose}
          aria-label="close"
        >
          <CloseIcon />
        </IconButton>
        <DialogTitle>
          Packs
        </DialogTitle>
      </Toolbar>
      <DialogContent>
        <DialogContentText sx={{ my: 2 }}>
          Download map data for use offline. Right now it&apos;s mostly just regions of California.
        </DialogContentText>

        <Box sx={{ display: 'flex', gap: 1, mb: locationError ? 1 : 2 }}>
          <TextField
            fullWidth
            size="small"
            type="search"
            placeholder="Search packs"
            value={searchText}
            onChange={event => setSearchText(event.target.value)}
            inputProps={{ 'aria-label': 'Search packs' }}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
              ),
              endAdornment: searchText && (
                <InputAdornment position="end">
                  <IconButton
                    edge="end"
                    size="small"
                    aria-label="Clear search"
                    onClick={() => setSearchText('')}
                  >
                    <ClearIcon fontSize="small" />
                  </IconButton>
                </InputAdornment>
              ),
            }}
          />
          <ToggleButton
            value="nearMe"
            size="small"
            selected={nearMe}
            onChange={toggleNearMe}
            sx={{ flexShrink: 0, gap: 0.5, px: 1.5 }}
          >
            {isLocating ? <CircularProgress size={20} /> : <MyLocationIcon fontSize="small" />}
            Near me
          </ToggleButton>
        </Box>
        {locationError && (
          <DialogContentText color="error" variant="body2" sx={{ mb: 2 }}>
            {locationError}
          </DialogContentText>
        )}

        <TabContext value={currentTab}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <TabList
              onChange={(_event: React.SyntheticEvent, newVal: 'all' | 'downloaded') => setCurrentTab(newVal)}
              aria-label="Packs"
            >
              <Tab label="All" value="all" />
              <Tab label="Downloaded" value="downloaded" />
            </TabList>
          </Box>
          <PackTab
            value="all"
            packs={matchingPacks}
            isOffline={isOffline}
            loading={loading}
            currentPackId={currentPackId}
            requestedPackId={requestedPackId}
            packStore={packStore}
            description={
              isOffline && !loading
                ? 'Looks like you\'re offline. You can choose packs you\'ve already downloaded or '
                + 'try again when you\'re online.'
                : isFiltering && !loading && matchingPacks?.length === 0
                  ? noMatchesDescription
                  : null
            }
            onChoose={onChoose}
            onDelete={refreshPacks}
            onDownload={refreshPacks}
          >
            {/* Filtering lists the matches flat, whatever level of the hierarchy they're on */}
            {packTree && !isFiltering
              ? (
                  <PackNodeList
                    currentNodeId={browseNodeId}
                    currentPackId={currentPackId}
                    packStore={packStore}
                    requestedPackId={requestedPackId}
                    tree={packTree}
                    onChoose={onChoose}
                    onDelete={refreshPacks}
                    onDownload={refreshPacks}
                    onNavigate={setBrowseNodeId}
                  />
                )
              : undefined}
          </PackTab>
          <PackTab
            value="downloaded"
            packs={matchingDownloadedPacks}
            isOffline={isOffline}
            loading={loadingLocal}
            currentPackId={currentPackId}
            requestedPackId={requestedPackId}
            packStore={packStore}
            headerAction={(
              <AddLocalPackButton
                packStore={packStore}
                onAdd={packId => {
                  setDownloadedPacks(null);
                  packStore.setCurrent(packId);
                  onChoose(packId);
                }}
              />
            )}
            description={
              !loadingLocal && (downloadedPacks === null || downloadedPacks?.length === 0)
                ? 'No packs downloaded yet.'
                : isFiltering && !loadingLocal && matchingDownloadedPacks?.length === 0
                  ? noMatchesDescription
                  : null
            }
            onChoose={onChoose}
            onDelete={refreshPacks}
            onDownload={refreshPacks}
          />
        </TabContext>
      </DialogContent>
    </>
  );
}
