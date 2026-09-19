import Fab from '@mui/material/Fab';
import TrendingDownIcon from '@mui/icons-material/TrendingDown';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';

import { TRACE_COLORS, TraceDirection } from './flowTrace';

const LABELS: Record<TraceDirection, string> = {
  downstream: 'Trace downstream',
  upstream: 'Trace upstream',
};

const ICONS: Record<TraceDirection, React.ReactNode> = {
  downstream: <TrendingDownIcon />,
  upstream: <TrendingUpIcon />,
};

// Stacked above CurrentLocationButton, with upstream on top
const BOTTOMS: Record<TraceDirection, number> = {
  downstream: 175,
  upstream: 245,
};

interface Props {
  direction: TraceDirection;
  // Whether the waterway under the crosshairs can be traced
  traceable: boolean;
  // Whether a trace is showing
  active: boolean;
  onClick: () => void;
}

// Toggles a trace of where water flows to or from the waterway under the
// crosshairs
const TraceButton = ({ direction, traceable, active, onClick }: Props) => {
  if (!traceable && !active) return null;
  return (
    <Fab
      aria-label={LABELS[direction]}
      aria-pressed={active}
      onClick={onClick}
      style={{
        position: 'absolute',
        right: 15,
        bottom: BOTTOMS[direction],
        backgroundColor: 'white',
        color: active ? TRACE_COLORS[direction] : undefined,
      }}
    >
      {ICONS[direction]}
    </Fab>
  );
};

export default TraceButton;
