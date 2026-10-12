import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import CircularProgress from '@mui/material/CircularProgress';
import DeleteIcon from '@mui/icons-material/Delete';
import FileDownloadIcon from '@mui/icons-material/FileDownload';
import FolderOutlinedIcon from '@mui/icons-material/FolderOutlined';
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
import { packCountLabel } from '../packs/packTree';
import { PackStore } from '../packs/types';

// Each line of a two-line secondary text truncates on its own, since the
// list item's noWrap only ellipsizes a single line of text
const secondaryLineSx = {
  display: 'block',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
} as const;

interface Props {
  currentPackId: string | null;
  onChoose?: (packId: string | null) => void;
  onDelete?: () => void | Promise<void>;
  onDownload?: () => void | Promise<void>;
  // Number of packs nested under this one. When positive, the row gets a button that opens them.
  nestedPackCount?: number;
  onOpen?: () => void;
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
  onOpen,
  nestedPackCount = 0,
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
      // This row stays mounted through the refresh, so stop showing progress now that the
      // refreshed pack list says the pack is downloaded
      .then(() => setDownloadProgress(null))
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
  const description = hasUpdate
    ? (
        <>
          <strong>Update Available</strong>
          {' '}
          {pack.description}
        </>
      )
    : pack.description;
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
  // A pack with packs inside it is opened, not acted on: no radio, and tapping the row or its
  // button goes in. Choosing, downloading, updating, and deleting it are on its own row inside,
  // so the way in gets the same weight the download button has.
  const opensNestedPacks = nestedPackCount > 0 && !!onOpen;
  if (opensNestedPacks) {
    secondaryAction = (
      <Button
        aria-label={`Show packs within ${pack.name}`}
        color="primary"
        onClick={onOpen}
        endIcon={<ChevronRightIcon />}
        sx={{
          'minWidth': 0,
          'whiteSpace': 'nowrap',
          'p': 1,
          // The same offset as the download button's edge="end", so the icons line up
          'mr': '-12px',
          // The icon alone on narrow screens, where the count would crowd the row. It's sized
          // like the download icon, not like a button's smaller end icon.
          '& .MuiButton-endIcon': {
            'display': 'flex',
            'm': { xs: 0, sm: '0 -4px 0 4px' },
            '& > *:nth-of-type(1)': { fontSize: 24 },
          },
        }}
      >
        <Box component="span" sx={{ display: { xs: 'none', sm: 'inline' } }}>
          {packCountLabel(nestedPackCount)}
        </Box>
      </Button>
    );
  }
  return (
    <ListItem
      key={pack.id}
      ref={itemRef}
      sx={{ pl: 0 }}
    >
      <ListItemButton
        disabled={!isDownloaded && !opensNestedPacks}
        // minWidth lets the name and description truncate to make room for the actions, whatever
        // width they end up
        sx={{ pl: 0, minWidth: 0 }}
        onClick={() => {
          if (opensNestedPacks) {
            onOpen();
            return;
          }
          packStore.setCurrent(pack.id);
          if (typeof (onChoose) === 'function') onChoose(pack.id);
        }}
      >
        <ListItemIcon sx={{ display: 'flex', justifyContent: 'center' }}>
          {opensNestedPacks
            ? <FolderOutlinedIcon />
            : (
                <Radio
                  edge="start"
                  checked={currentPackId === pack.id}
                  disableRipple
                />
              )}
        </ListItemIcon>
        <ListItemText
          primary={pack.name}
          primaryTypographyProps={{
            noWrap: true,
          }}
          secondary={
            downloadPercent !== null
              ? `${downloadPercent}% downloaded...`
              : pack.sourceFileName
                ? (
                    <>
                      {description && <Box component="span" sx={secondaryLineSx}>{description}</Box>}
                      <Box component="span" sx={secondaryLineSx}>
                        {`Loaded from ${pack.sourceFileName}`}
                      </Box>
                    </>
                  )
                : description
          }
          secondaryTypographyProps={{
            noWrap: true,
          }}
        />
      </ListItemButton>
      <Box sx={{ display: 'flex', alignItems: 'center', flexShrink: 0 }}>
        {secondaryAction}
      </Box>
    </ListItem>
  );
};

export default PackListItem;
