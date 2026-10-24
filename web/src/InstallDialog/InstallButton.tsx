import Button from '@mui/material/Button';
import InstallMobileIcon from '@mui/icons-material/InstallMobile';

import { useCanInstall, useInstall } from './useInstall';

export default function InstallButton() {
  const canInstall = useCanInstall();
  const install = useInstall();
  if (!canInstall) return null;
  return (
    <Button startIcon={<InstallMobileIcon />} onClick={install}>
      Install App
    </Button>
  );
}
