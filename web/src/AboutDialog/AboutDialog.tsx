import CloseIcon from '@mui/icons-material/Close';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IconButton from '@mui/material/IconButton';
import Toolbar from '@mui/material/Toolbar';
import useMediaQuery from '@mui/material/useMediaQuery';
import { useTheme } from '@mui/material/styles';

import { useHideAboutModal, useAboutModalShown } from '../useAppStore';

export default function PacksDialog() {
  const aboutModalShown = useAboutModalShown();
  const hideAboutModal = useHideAboutModal();
  const theme = useTheme();
  const isSmall = useMediaQuery(theme.breakpoints.down('md'));
  return (
    <Dialog
      open={aboutModalShown}
      fullScreen={isSmall}
      fullWidth
      maxWidth="lg"
      onClose={hideAboutModal}
    >
      <Toolbar>
        <IconButton
          edge="start"
          color="inherit"
          onClick={hideAboutModal}
          aria-label="close"
        >
          <CloseIcon />
        </IconButton>
        <DialogTitle sx={{ flex: 1 }}> About Underfoot </DialogTitle>
      </Toolbar>
      <DialogContent>
        <p>
          Underfoot is an app for viewing geologic and hydrologic maps made by
          me, <a href="https://kueda.net">Ken-ichi</a>. I designed it to be
          offline-first, which means it's a little different than some
          other web sites and apps. Now that you've visited it once, you can
          come back here using the same URL in your browser, even if you're
          offline. Or, if you want, your browser might let you save this
          website as an app (it's a button in the address bar in desktop
          Chrome; mobile browsers often call it "Add to home screen" or
          something like that).
        </p>

        <p>
          And since it works offline, you need to download data before you can
          do anything. Just like you might do when downloading offline tiles
          from Google Maps, you need to download a subset a map data before
          Underfoot can show you anything.
        </p>

        <p>
          This is a hobby project so it's kind of forever <strong>🚧 under construction 🚧</strong>.
          If you're curious, the&nbsp;
          <a href="https://github.com/kueda/underfoot">data prep code</a> and&nbsp;
          <a href="https://github.com/kueda/underfoot-web">web app code</a> are open-source.
        </p>

        <p>Some notes on sources and attribution:</p>

        <ul>
          <li>
            <p>
              <a href="https://ngmdb.usgs.gov/ngmdb/ngmdb_home.html">USGS geologic maps</a> &amp;
              <a href="https://www.usgs.gov/national-hydrography/nhdplus-high-resolution">NHDPlus HR</a> (National
              Hydrography Dataset): these amazing datasets make
              things like this possible. Amazing stuff from the United States
              federal government, one of the coolest uses of my tax dollars.
              Please digitize more, USGS!
            </p>
          </li>
          <li>
            <p>
              <a href="https://www.openstreetmap.org">OpenStreetMap</a> data
              hosted at <a href="https://download.geofabrik.de/">GEOFABRIK</a>. This is
              where all the streets, peaks, ridges, and springs come from.
              Best thing on the Internet after Wikipedia.
            </p>
          </li>
          <li>
            <p>
              <a href="https://registry.opendata.aws/terrain-tiles/">Mapzen Terrain Tiles</a>, hosted by the&nbsp;
              <a href="https://aws.amazon.com/opendata/open-data-sponsorship-program/">Amazon Open Data Sponsorship Program</a>.&nbsp;
              Using this for the contour lines.
            </p>
          </li>
          <li>
            <p>
              <a href="https://github.com/vite-pwa/vite-plugin-pwa">vite-plugin-pwa</a>:
              the main tool for turning a simple React app into a Progressive Web App.
            </p>
          </li>
        </ul>
      </DialogContent>
    </Dialog>
  );
}
