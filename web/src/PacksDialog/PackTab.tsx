import CircularProgress from '@mui/material/CircularProgress';
import DialogContentText from '@mui/material/DialogContentText';
import List from '@mui/material/List';
import TabPanel from '@mui/lab/TabPanel';
import { ReactNode } from 'react';

import { PackStore } from '../packs/types';
import { Pack } from '../packs/Pack';
import PackListItem from './PackListItem';

interface PackTabProps {
  currentPackId: string | null;
  description: string | null;
  headerAction?: ReactNode;
  isOffline: boolean;
  loading: boolean;
  onChoose: (packId: string | null) => void;
  onDelete: () => void | Promise<void>;
  onDownload: () => void | Promise<void>;
  packs: Pack[] | null;
  packStore: PackStore;
  requestedPackId?: string | null;
  value: string;
}

const PackTab = ({
  currentPackId,
  description,
  headerAction,
  loading,
  onChoose,
  onDelete,
  onDownload,
  packs,
  packStore,
  requestedPackId,
  value,
}: PackTabProps) => (
  <TabPanel value={value} sx={{ padding: 0 }}>
    {headerAction}
    {loading && <CircularProgress />}
    {description && (
      <DialogContentText sx={{ textAlign: 'center', p: 4 }}>
        {description}
      </DialogContentText>
    )}
    <List>
      { packs?.map(pack => (
        <PackListItem
          key={pack.id}
          currentPackId={currentPackId}
          pack={pack}
          packStore={packStore}
          requested={pack.id === requestedPackId}
          onChoose={onChoose}
          onDelete={onDelete}
          onDownload={onDownload}
        />
      )) }
    </List>
  </TabPanel>
);

export default PackTab;
