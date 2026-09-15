import BackpackIcon from '@mui/icons-material/Backpack';
import Box from '@mui/material/Box';
import BugReportIcon from '@mui/icons-material/BugReport';
import Divider from '@mui/material/Divider';
import Drawer from '@mui/material/Drawer';
import HistoryIcon from '@mui/icons-material/History';
import LandscapeIcon from '@mui/icons-material/Landscape';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import WaterIcon from '@mui/icons-material/Water';
import InfoIcon from '@mui/icons-material/Info';

import { reportBug } from './reportBug';
import {
  ROCKS,
  WATER,
  useLogging,
  useMapType,
  useSetMapType,
  useShowPacksModal,
  useShowAboutModal,
} from './useAppStore';

interface DrawerProps {
  drawerOpen: boolean;
  setDrawerOpen: (newVal: boolean) => void;
}

export default function UnderfootDrawer({
  drawerOpen,
  setDrawerOpen,
}: DrawerProps) {
  const showPacksModal = useShowPacksModal();
  const showAboutModal = useShowAboutModal();
  const mapType = useMapType();
  const setMapType = useSetMapType();
  const { showLogModal } = useLogging();
  return (
    <>
      <Drawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
      >
        <Box sx={{ width: 250 }} role="presentation" onClick={() => setDrawerOpen(false)}>
          <List>
            <ListItem disablePadding>
              <ListItemButton selected={mapType === ROCKS} onClick={() => setMapType(ROCKS)}>
                <ListItemIcon>
                  <LandscapeIcon />
                </ListItemIcon>
                <ListItemText primary="Rocks" />
              </ListItemButton>
            </ListItem>
            <ListItem disablePadding>
              <ListItemButton selected={mapType === WATER} onClick={() => setMapType(WATER)}>
                <ListItemIcon>
                  <WaterIcon />
                </ListItemIcon>
                <ListItemText primary="Water" />
              </ListItemButton>
            </ListItem>
          </List>
          <Divider />
          <List>
            <ListItem disablePadding>
              <ListItemButton onClick={showPacksModal}>
                <ListItemIcon>
                  <BackpackIcon />
                </ListItemIcon>
                <ListItemText primary="Packs" />
              </ListItemButton>
            </ListItem>
            <ListItem disablePadding>
              <ListItemButton onClick={showLogModal}>
                <ListItemIcon>
                  <HistoryIcon />
                </ListItemIcon>
                <ListItemText primary="Debug Log" />
              </ListItemButton>
            </ListItem>
            <ListItem disablePadding>
              <ListItemButton onClick={reportBug}>
                <ListItemIcon>
                  <BugReportIcon />
                </ListItemIcon>
                <ListItemText primary="Report a Bug" />
              </ListItemButton>
            </ListItem>
            <ListItem disablePadding>
              <ListItemButton onClick={showAboutModal}>
                <ListItemIcon>
                  <InfoIcon />
                </ListItemIcon>
                <ListItemText primary="About" />
              </ListItemButton>
            </ListItem>
          </List>
        </Box>
      </Drawer>
    </>
  );
}
