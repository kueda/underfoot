import { act, fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const IPHONE_SAFARI = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) '
  + 'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1';
const MAC_FIREFOX = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) '
  + 'Gecko/20100101 Firefox/141.0';

function stubDisplayMode(standalone: boolean) {
  vi.stubGlobal('matchMedia', (query: string) => ({
    matches: standalone && query === '(display-mode: standalone)',
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  }));
}

// What Chromium browsers fire when they're willing to install the app
function fireBeforeInstallPrompt() {
  const event = new Event('beforeinstallprompt', { cancelable: true });
  const prompt = vi.fn(() => Promise.resolve());
  Object.assign(event, {
    prompt,
    userChoice: Promise.resolve({ outcome: 'accepted', platform: 'web' }),
  });
  act(() => {
    window.dispatchEvent(event);
  });
  return { event, prompt };
}

// Install state lives in a module-level store, so load fresh modules for each test
async function renderInstallButton() {
  vi.resetModules();
  const { useInstallListeners } = await import('./useInstall');
  const { default: InstallButton } = await import('./InstallButton');
  const { default: InstallDialog } = await import('./InstallDialog');
  function App() {
    useInstallListeners();
    return (
      <>
        <InstallButton />
        <InstallDialog />
      </>
    );
  }
  render(<App />);
}

function installButton() {
  return screen.queryByRole('button', { name: 'Install App' });
}

describe('InstallButton', () => {
  beforeEach(() => {
    stubDisplayMode(false);
  });

  it('is hidden when already running as an installed app', async () => {
    stubDisplayMode(true);
    await renderInstallButton();
    expect(installButton()).toBeNull();
  });

  it('shows the browser install prompt when the browser offers one', async () => {
    await renderInstallButton();
    const { event, prompt } = fireBeforeInstallPrompt();
    expect(event.defaultPrevented).toBe(true);
    fireEvent.click(installButton()!);
    expect(prompt).toHaveBeenCalledOnce();
    expect(screen.queryByRole('dialog')).toBeNull();
    // Accepting installs the app, so there's nothing left to offer
    await act(() => Promise.resolve());
    expect(installButton()).toBeNull();
  });

  it('explains how to add to the home screen on iOS', async () => {
    vi.spyOn(navigator, 'userAgent', 'get').mockReturnValue(IPHONE_SAFARI);
    await renderInstallButton();
    fireEvent.click(installButton()!);
    expect(screen.getByRole('dialog').textContent).toContain('Add to Home Screen');
  });

  it('explains what to do in browsers that cannot install apps', async () => {
    vi.spyOn(navigator, 'userAgent', 'get').mockReturnValue(MAC_FIREFOX);
    await renderInstallButton();
    fireEvent.click(installButton()!);
    expect(screen.getByRole('dialog').textContent).toContain('bookmark');
  });

  it('closes the instructions', async () => {
    await renderInstallButton();
    fireEvent.click(installButton()!);
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    // MUI keeps the dialog in the DOM while it fades out
    await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
  });

  it('hides once the app is installed', async () => {
    await renderInstallButton();
    act(() => {
      window.dispatchEvent(new Event('appinstalled'));
    });
    expect(installButton()).toBeNull();
  });
});
