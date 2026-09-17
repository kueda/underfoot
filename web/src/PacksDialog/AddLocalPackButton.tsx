import Button from '@mui/material/Button';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import { useRef } from 'react';

import { PackStore } from '../packs/types';

interface Props {
  onAdd: (packId: string) => void;
  packStore: PackStore;
}

// Lets a pack built locally with `packs.py` be viewed without hosting it
// anywhere first: pick its .zip and it's unzipped straight into packStore,
// same as a downloaded pack.
const AddLocalPackButton = ({ onAdd, packStore }: Props) => {
  const inputRef = useRef<HTMLInputElement>(null);
  return (
    <>
      <input
        ref={inputRef}
        type="file"
        accept=".zip"
        hidden
        onChange={e => {
          const file = e.target.files?.[0];
          // Let the same file be picked again later without a reload
          e.target.value = '';
          if (!file) return;
          packStore.addFromFile(file)
            .then(onAdd)
            .catch(err => console.error('Failed to add local pack', err));
        }}
      />
      <Button
        startIcon={<UploadFileIcon />}
        onClick={() => inputRef.current?.click()}
        sx={{ m: 1 }}
      >
        Load pack from file&hellip;
      </Button>
    </>
  );
};

export default AddLocalPackButton;
