import React from 'react';
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';

import IconButton from '@mui/material/IconButton';
import MenuIcon from '@mui/icons-material/Menu';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';

import Drawer from './Drawer';

export default function UnderfootAppBar() {
  const [drawerOpen, setDrawerOpen] = React.useState(false);

  const handleDrawerToggle = () => {
    setDrawerOpen(prevState => !prevState);
  };

  return (
    <>
      <Drawer
        drawerOpen={drawerOpen}
        setDrawerOpen={setDrawerOpen}
      />
      <Box sx={{ display: 'flex' }}>
        <AppBar component="nav">
          <Toolbar>
            <IconButton
              color="inherit"
              aria-label="open drawer"
              edge="start"
              onClick={handleDrawerToggle}
              sx={{ mr: 2 }}
            >
              <MenuIcon />
            </IconButton>
            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
              Underfoot
            </Typography>
          </Toolbar>
        </AppBar>
      </Box>
    </>
  );
}
