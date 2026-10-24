import Box from '@mui/material/Box';
import ClearIcon from '@mui/icons-material/Clear';
import CloseIcon from '@mui/icons-material/Close';
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
import Toolbar from '@mui/material/Toolbar';
import {
  useCallback, useEffect, useMemo, useRef, useState,
} from 'react';
import { Pack } from '../packs/Pack';
import { buildPackTree } from '../packs/packTree';
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
  const packTree = useMemo(() => (packs ? buildPackTree(packs) : null), [packs]);

  const matchingPacks = useMemo(() => filterPacks(packs, searchText), [packs, searchText]);
  const matchingDownloadedPacks = useMemo(
    () => filterPacks(downloadedPacks, searchText),
    [downloadedPacks, searchText],
  );
  const noMatchesDescription = `No packs match "${searchText.trim()}".`;

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
  const showRequestedPackPrompt = !!requestedPackId && !requestedPack?.data;

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

        <TextField
          fullWidth
          size="small"
          type="search"
          placeholder="Search packs"
          value={searchText}
          onChange={event => setSearchText(event.target.value)}
          sx={{ mb: 2 }}
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
                : isSearching && !loading && matchingPacks?.length === 0
                  ? noMatchesDescription
                  : null
            }
            onChoose={onChoose}
            onDelete={refreshPacks}
            onDownload={refreshPacks}
          >
            {/* Searching lists the matches flat, whatever level of the hierarchy they're on */}
            {packTree && !isSearching
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
                : isSearching && !loadingLocal && matchingDownloadedPacks?.length === 0
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
