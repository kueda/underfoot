import Dialog from '@mui/material/Dialog';
import useMediaQuery from '@mui/material/useMediaQuery';
import { useTheme } from '@mui/material/styles';

import { useHidePacksModal, usePacksModalShown } from '../useAppStore';
import Packs from './Packs';

export default function PacksDialog() {
  const packsModalShown = usePacksModalShown();
  const hidePacksModal = useHidePacksModal();
  const theme = useTheme();
  const isSmall = useMediaQuery(theme.breakpoints.down('md'));
  return (
    <Dialog
      open={packsModalShown}
      fullScreen={isSmall}
      fullWidth
      maxWidth="lg"
      onClose={hidePacksModal}
    >
      <Packs />
    </Dialog>
  );
}
