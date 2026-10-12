import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import FolderOutlinedIcon from '@mui/icons-material/FolderOutlined';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';

import { PackNode, packCountLabel } from '../packs/packTree';

interface Props {
  node: PackNode;
  onOpen: () => void;
}

// A place with packs under it but no pack of its own, like a state we only have parks for
const PackGroupItem = ({ node, onOpen }: Props) => (
  <ListItem disablePadding>
    <ListItemButton sx={{ pl: 0 }} onClick={onOpen}>
      <ListItemIcon sx={{ display: 'flex', justifyContent: 'center' }}>
        <FolderOutlinedIcon />
      </ListItemIcon>
      <ListItemText
        primary={node.label}
        secondary={packCountLabel(node.descendantPackCount)}
      />
      <ChevronRightIcon />
    </ListItemButton>
  </ListItem>
);

export default PackGroupItem;
