// What to tell someone when a pack couldn't be downloaded or loaded. Running
// out of storage is worth calling out, since they can do something about it.
export function packErrorMessage(error: unknown, fallback: string) {
  if ((error as { name?: string } | null)?.name === 'QuotaExceededError') {
    return 'There is not enough storage space for this pack. Delete other packs or free up '
      + 'space on this device, then try again.';
  }
  return fallback;
}
