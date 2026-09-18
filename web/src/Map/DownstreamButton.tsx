import Fab from '@mui/material/Fab';
import TrendingDownIcon from '@mui/icons-material/TrendingDown';

import { DOWNSTREAM_COLOR } from './flowTrace';

interface Props {
  // Whether the waterway under the crosshairs can be traced
  traceable: boolean;
  // Whether a trace is showing
  active: boolean;
  onClick: () => void;
}

// Toggles a trace of where water flows from the waterway under the crosshairs
const DownstreamButton = ({ traceable, active, onClick }: Props) => {
  if (!traceable && !active) return null;
  return (
    <Fab
      aria-label="Trace downstream"
      aria-pressed={active}
      onClick={onClick}
      // Stacked above CurrentLocationButton
      style={{
        position: 'absolute',
        right: 15,
        bottom: 175,
        backgroundColor: 'white',
        color: active ? DOWNSTREAM_COLOR : undefined,
      }}
    >
      <TrendingDownIcon />
    </Fab>
  );
};

export default DownstreamButton;
