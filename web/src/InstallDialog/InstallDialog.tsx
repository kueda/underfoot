import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import IosShareIcon from '@mui/icons-material/IosShare';
import MoreVertIcon from '@mui/icons-material/MoreVert';

import { installPlatform } from './installPlatform';
import { hideInstallDialog, useInstallDialogShown } from './useInstall';

const inlineIcon = { fontSize: 'inherit', verticalAlign: 'text-bottom' };

function Instructions() {
  switch (installPlatform()) {
    case 'ios':
      return (
        <ol>
          <li>
            Tap the Share button <IosShareIcon sx={inlineIcon} aria-label="Share" />.
            In Safari you might need to tap the "•••" button first.
          </li>
          <li>Tap <strong>Add to Home Screen</strong>. You might need to scroll down to see it.</li>
          <li>Tap <strong>Add</strong>.</li>
        </ol>
      );
    case 'android':
      return (
        <ol>
          <li>Open your browser's menu <MoreVertIcon sx={inlineIcon} aria-label="menu" />.</li>
          <li>Tap <strong>Install</strong> or <strong>Add to Home screen</strong>.</li>
        </ol>
      );
    case 'mac-safari':
      return (
        <ol>
          <li>
            Choose <strong>File &gt; Add to Dock</strong>, or click the Share
            button <IosShareIcon sx={inlineIcon} aria-label="Share" /> and
            choose <strong>Add to Dock</strong>.
          </li>
        </ol>
      );
    case 'desktop-chromium':
      return (
        <p>
          Look for an install button at the right end of the address bar, or
          find <strong>Install Underfoot</strong> in your browser's menu (it
          might be under <strong>Cast, save, and share</strong> or
          <strong> Apps</strong>).
        </p>
      );
    default:
      return (
        <p>
          This browser can't install web apps, but Underfoot still works
          offline in it, so you could bookmark this page. Chrome, Edge, and
          Safari can install it as an app.
        </p>
      );
  }
}

export default function InstallDialog() {
  const installDialogShown = useInstallDialogShown();
  return (
    <Dialog open={installDialogShown} onClose={hideInstallDialog}>
      <DialogTitle>Install Underfoot</DialogTitle>
      <DialogContent>
        <p>
          You can install Underfoot as an app so it opens from your home screen
          or dock like any other app, even when you're offline.
        </p>
        <Instructions />
      </DialogContent>
      <DialogActions>
        <Button onClick={hideInstallDialog}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}
