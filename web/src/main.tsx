import React from 'react';
import ReactDOM from 'react-dom/client';
// @ts-expect-error Virtual modules seem to confuse TS
import { registerSW } from 'virtual:pwa-register';

import App from './App.tsx';
import './index.css';

import '@fontsource/roboto/300.css';
import '@fontsource/roboto/400.css';
import '@fontsource/roboto/500.css';
import '@fontsource/roboto/700.css';

if ('serviceWorker' in navigator) {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const updateSW = registerSW({
    onNeedRefresh() {
      if (confirm('A new version is availble. Do you want to update?')) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-call
        updateSW(true);
      }
    },
    onOfflineReady() {
      // alert('Ready to work offline');
    },
  });
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
