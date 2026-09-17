import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import DeleteIcon from '@mui/icons-material/Delete';
import FileDownloadIcon from '@mui/icons-material/FileDownload';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Radio from '@mui/material/Radio';
import { Refresh } from '@mui/icons-material';
import StopIcon from '@mui/icons-material/Stop';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Tooltip from '@mui/material/Tooltip';
import { useEffect, useRef, useState } from 'react';

import { Pack } from '../packs/Pack';
import { PackStore } from '../packs/types';

interface Props {
  currentPackId: string | null;
  onChoose?: (packId: string | null) => void;
  onDelete?: () => void;
  onDownload?: () => void;
  pack: Pack;
  packStore: PackStore;
  // A shared link points at this pack: scroll it into view and prompt to download.
  requested?: boolean;
}

const PackListItem = ({
  currentPackId,
  onChoose,
  onDelete,
  onDownload,
  pack,
  packStore,
  requested,
}: Props) => {
  const isDownloaded = !!pack.data;
  const hasUpdate = isDownloaded
    && !!pack.downloadedAt
    && pack.updatedAt > pack.downloadedAt;
  const [downloadProgress, setDownloadProgress] = useState<null | { loadedBytes: number; totalBytes: number }>(null);
  const [abortController, setAbortController] = useState(new AbortController());
  const [menuAnchor, setMenuAnchor] = useState<null | HTMLElement>(null);
  const longPressTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const longPressTriggered = useRef(false);
  const itemRef = useRef<HTMLLIElement>(null);
  // Show the "download this to open the shared map" prompt only while the linked
  // pack actually needs downloading.
  const showDownloadPrompt = !!requested && !isDownloaded && !downloadProgress;

  useEffect(() => {
    if (!showDownloadPrompt) return;
    // Let the dialog's open transition settle before scrolling so the row lands
    // centered rather than offset by the in-flight transform.
    const timer = setTimeout(() => {
      itemRef.current?.scrollIntoView({ block: 'center', behavior: 'smooth' });
    }, 150);
    return () => clearTimeout(timer);
  }, [showDownloadPrompt]);
  const handleDownload = () => {
    const ac = new AbortController();
    setAbortController(ac);
    packStore.download(pack.id, { onProgress: setDownloadProgress, signal: ac.signal })
      .then(() => (typeof (onDownload) === 'function' ? onDownload() : null))
      .then(() => (typeof (onChoose) === 'function' ? onChoose(pack.id) : null))
      .catch((e: Error) => {
        if (e?.message?.match(/aborted/)) {
          setDownloadProgress(null);
          return;
        }
        console.error('Failed to download pack', e);
      });
  };
  const downloadPercent = downloadProgress
    ? Math.round(downloadProgress.loadedBytes / downloadProgress.totalBytes * 100)
    : null;
  let secondaryAction;
  if (downloadProgress) {
    secondaryAction = (
      <Box sx={{ position: 'relative', display: 'inline-flex', mr: -1.5 }}>
        <CircularProgress variant="determinate" value={downloadPercent ?? 0} />
        <IconButton
          edge="end"
          aria-label="stop"
          onClick={() => abortController.abort()}
          sx={{
            position: 'absolute',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <StopIcon color="primary" />
        </IconButton>
      </Box>
    );
  }
  else if (isDownloaded) {
    const deleteButton = (
      <IconButton
        edge="end"
        aria-label="delete"
        onClick={() => {
          packStore.remove(pack.id)
            .then(() => (typeof (onDelete) === 'function' ? onDelete() : null))
            .then(() => (typeof (onChoose) === 'function' ? onChoose(null) : null))
            .catch(e => console.error('Problem deleting pack: ', e));
        }}
      >
        <DeleteIcon />
      </IconButton>
    );
    secondaryAction = hasUpdate
      ? (
          <>
            <IconButton
              edge="end"
              aria-label="update"
              onPointerDown={e => {
                longPressTriggered.current = false;
                const target = e.currentTarget;
                longPressTimer.current = setTimeout(() => {
                  longPressTriggered.current = true;
                  setMenuAnchor(target);
                }, 500);
              }}
              onPointerUp={() => {
                if (longPressTimer.current) clearTimeout(longPressTimer.current);
              }}
              onClick={() => {
                if (!longPressTriggered.current) handleDownload();
              }}
            >
              <Refresh />
            </IconButton>
            <Menu
              anchorEl={menuAnchor}
              open={!!menuAnchor}
              onClose={() => setMenuAnchor(null)}
            >
              <MenuItem
                onClick={() => {
                  setMenuAnchor(null);
                  packStore.remove(pack.id)
                    .then(() => (typeof (onDelete) === 'function' ? onDelete() : null))
                    .then(() => (typeof (onChoose) === 'function' ? onChoose(null) : null))
                    .catch(e => console.error('Problem deleting pack: ', e));
                }}
              >
                <ListItemIcon><DeleteIcon /></ListItemIcon>
                Delete
              </MenuItem>
            </Menu>
          </>
        )
      : deleteButton;
  }
  else {
    secondaryAction = (
      <Tooltip
        open={showDownloadPrompt}
        arrow
        placement="left"
        title="Download this pack to open the shared map"
        disableFocusListener
        disableHoverListener
        disableTouchListener
        slotProps={{
          tooltip: {
            sx: {
              'maxWidth': 180,
              'fontSize': '0.8rem',
              'bgcolor': 'warning.main',
              '& .MuiTooltip-arrow': { color: 'warning.main' },
            },
          },
        }}
      >
        <IconButton
          edge="end"
          color="primary"
          aria-label="download"
          onClick={handleDownload}
        >
          <FileDownloadIcon />
        </IconButton>
      </Tooltip>
    );
  }
  return (
    <ListItem
      key={pack.id}
      ref={itemRef}
      sx={{ pl: 0 }}
      secondaryAction={secondaryAction}
    >
      <ListItemButton
        disabled={!isDownloaded}
        sx={{ pl: 0, paddingRight: '0 !important' }}
        onClick={() => {
          packStore.setCurrent(pack.id);
          if (typeof (onChoose) === 'function') onChoose(pack.id);
        }}
      >
        <ListItemIcon sx={{ display: 'flex', justifyContent: 'center' }}>
          <Radio
            edge="start"
            checked={currentPackId === pack.id}
            disableRipple
          />
        </ListItemIcon>
        <ListItemText
          primary={pack.name}
          primaryTypographyProps={{
            noWrap: true,
          }}
          secondary={
            downloadPercent !== null
              ? `${downloadPercent}% downloaded...`
              : hasUpdate
                ? (
                    <>
                      <strong>Update Available</strong>
                      {' '}
                      {pack.description}
                    </>
                  )
                : pack.description
          }
          secondaryTypographyProps={{
            noWrap: true,
          }}
        />
      </ListItemButton>
    </ListItem>
  );
};

export default PackListItem;
