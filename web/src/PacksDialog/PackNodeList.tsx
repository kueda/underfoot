import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import Box from '@mui/material/Box';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import Link from '@mui/material/Link';
import List from '@mui/material/List';
import Typography from '@mui/material/Typography';

import { PackStore } from '../packs/types';
import { PackNode, PackTree } from '../packs/packTree';
import PackGroupItem from './PackGroupItem';
import PackListItem from './PackListItem';

interface Props {
  currentNodeId: string | null;
  currentPackId: string | null;
  onChoose: (packId: string | null) => void;
  onDelete: () => void | Promise<void>;
  onDownload: () => void | Promise<void>;
  // Null goes back to the top level
  onNavigate: (nodeId: string | null) => void;
  packStore: PackStore;
  requestedPackId?: string | null;
  tree: PackTree;
}

// One level of the pack hierarchy: the top level, or what's inside the node at currentNodeId
const PackNodeList = ({
  currentNodeId,
  currentPackId,
  onChoose,
  onDelete,
  onDownload,
  onNavigate,
  packStore,
  requestedPackId,
  tree,
}: Props) => {
  const currentNode = currentNodeId ? tree.get(currentNodeId) : undefined;
  const nodes = currentNode ? currentNode.children : tree.roots;

  const ancestors: PackNode[] = [];
  let ancestor = currentNode?.parentId ? tree.get(currentNode.parentId) : undefined;
  while (ancestor) {
    ancestors.unshift(ancestor);
    ancestor = ancestor.parentId ? tree.get(ancestor.parentId) : undefined;
  }

  const renderPack = (node: PackNode, nested: boolean) => node.pack && (
    <PackListItem
      key={node.id}
      currentPackId={currentPackId}
      nestedPackCount={nested ? node.descendantPackCount : 0}
      pack={node.pack}
      packStore={packStore}
      requested={node.pack.id === requestedPackId}
      onChoose={onChoose}
      onDelete={onDelete}
      onDownload={onDownload}
      onOpen={() => onNavigate(node.id)}
    />
  );

  return (
    <>
      {currentNode && (
        <Box sx={{ display: 'flex', alignItems: 'center', py: 1 }}>
          <IconButton
            edge="start"
            aria-label="Back"
            onClick={() => onNavigate(currentNode.parentId)}
          >
            <ArrowBackIcon />
          </IconButton>
          <Breadcrumbs sx={{ ml: 1 }}>
            <Link component="button" underline="hover" onClick={() => onNavigate(null)}>
              All packs
            </Link>
            {ancestors.map(node => (
              <Link
                key={node.id}
                component="button"
                underline="hover"
                onClick={() => onNavigate(node.id)}
              >
                {node.label}
              </Link>
            ))}
            <Typography color="text.primary">{currentNode.label}</Typography>
          </Breadcrumbs>
        </Box>
      )}
      <List>
        {currentNode?.pack && (
          <>
            {renderPack(currentNode, false)}
            <Divider component="li" />
          </>
        )}
        {nodes.map(node => (node.pack
          ? renderPack(node, node.children.length > 0)
          : (
              <PackGroupItem
                key={node.id}
                node={node}
                onOpen={() => onNavigate(node.id)}
              />
            )))}
      </List>
    </>
  );
};

export default PackNodeList;
