// Platforms that need different instructions for installing the app when the
// browser can't show its own install prompt
export type InstallPlatform =
  | 'ios'
  | 'android'
  | 'mac-safari'
  | 'desktop-chromium'
  | 'other';

export function installPlatform(
  userAgent = navigator.userAgent,
  maxTouchPoints = navigator.maxTouchPoints,
): InstallPlatform {
  if (/iPhone|iPad|iPod/.test(userAgent)) return 'ios';
  // iPadOS Safari reports itself as a Mac, but Macs don't have touch screens
  if (/Macintosh/.test(userAgent) && maxTouchPoints > 1) return 'ios';
  if (/Android/.test(userAgent)) return 'android';
  if (/Chrome|Chromium|Edg\//.test(userAgent)) return 'desktop-chromium';
  if (/Macintosh/.test(userAgent) && /Safari/.test(userAgent)) return 'mac-safari';
  return 'other';
}
