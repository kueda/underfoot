import React from 'react';
import { create } from 'zustand';

// Chromium-only event, so it's not in TypeScript's DOM types
interface BeforeInstallPromptEvent extends Event {
  prompt: () => Promise<void>;
  userChoice: Promise<{ outcome: 'accepted' | 'dismissed'; platform: string }>;
}

interface InstallState {
  installed: boolean;
  // Saved so the browser's install prompt can be shown when the user asks
  installPrompt: BeforeInstallPromptEvent | null;
  installDialogShown: boolean;
}

const useInstallStore = create<InstallState>(() => ({
  installed: false,
  installPrompt: null,
  installDialogShown: false,
}));

function isStandalone() {
  return window.matchMedia('(display-mode: standalone)').matches
    // iOS Safari doesn't support the display-mode media query
    || (navigator as Navigator & { standalone?: boolean }).standalone === true;
}

// Call once, as early as possible, since browsers can offer to install before
// anything that shows install options has rendered
export function useInstallListeners() {
  React.useEffect(() => {
    useInstallStore.setState({ installed: isStandalone() });
    const handleBeforeInstallPrompt = (event: Event) => {
      // Keep the browser from showing its own install UI so we can show it
      // when the user asks for it
      event.preventDefault();
      useInstallStore.setState({ installPrompt: event as BeforeInstallPromptEvent });
    };
    const handleAppInstalled = () => useInstallStore.setState({ installed: true });
    window.addEventListener('beforeinstallprompt', handleBeforeInstallPrompt);
    window.addEventListener('appinstalled', handleAppInstalled);
    return () => {
      window.removeEventListener('beforeinstallprompt', handleBeforeInstallPrompt);
      window.removeEventListener('appinstalled', handleAppInstalled);
    };
  }, []);
}

// Shows the browser's install prompt if it has one, otherwise instructions
async function install() {
  const { installPrompt } = useInstallStore.getState();
  if (!installPrompt) {
    useInstallStore.setState({ installDialogShown: true });
    return;
  }
  // A prompt can only be shown once
  useInstallStore.setState({ installPrompt: null });
  await installPrompt.prompt();
  const { outcome } = await installPrompt.userChoice;
  if (outcome === 'accepted') useInstallStore.setState({ installed: true });
}

export const useCanInstall = () => useInstallStore(s => !s.installed);
export const useInstall = () => () => {
  install().catch(e => console.error('Failed to prompt for install', e));
};
export const useInstallDialogShown = () => useInstallStore(s => s.installDialogShown);
export const hideInstallDialog = () => useInstallStore.setState({ installDialogShown: false });
