import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import { useRef, useState } from 'react';

import { packErrorMessage } from '../packs/packErrorMessage';
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
  const [error, setError] = useState<string | null>(null);
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
          setError(null);
          packStore.addFromFile(file)
            .then(onAdd)
            .catch(err => {
              console.error('Failed to add local pack', err);
              setError(packErrorMessage(err, `Could not load ${file.name}. Is it a pack .zip?`));
            });
        }}
      />
      <Button
        startIcon={<UploadFileIcon />}
        onClick={() => inputRef.current?.click()}
        sx={{ m: 1 }}
      >
        Load pack from file&hellip;
      </Button>
      {error && (
        <Typography role="alert" color="error" variant="body2" sx={{ mx: 1, mb: 1 }}>
          {error}
        </Typography>
      )}
    </>
  );
};

export default AddLocalPackButton;
