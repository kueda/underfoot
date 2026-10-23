// The Umami analytics tracker loaded by index.html. It's undefined when the
// script is blocked or hasn't loaded, e.g. offline.
interface Window {
  umami?: {
    track: (eventName: string, eventData?: Record<string, string | number | boolean>) => void;
  };
}
