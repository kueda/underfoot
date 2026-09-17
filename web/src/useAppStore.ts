import { create } from 'zustand';

interface CurrentPackState {
  currentPackId: string | null;
  setCurrentPackId: (id: string | null) => void;
}

export const ROCKS = 'rocks';
export const WATER = 'water';

type MapType = typeof ROCKS | typeof WATER;

interface MapTypeState {
  mapType: MapType;
  setMapType: (type: MapType) => void;
}

interface PacksModalState {
  packsModalShown: boolean;
  // Pack the user should be nudged to download, e.g. when a shared link points
  // to a pack that isn't downloaded yet.
  requestedPackId: string | null;
  showPacksModal: () => void;
  // Opens the packs modal and highlights a specific pack to download.
  requestPack: (packId: string) => void;
  hidePacksModal: () => void;
}

interface LogEntry {
  body: string;
  date: Date;
}

interface LogState {
  log: LogEntry[];
  logModalShown: boolean;
  addLog: (message: string) => void;
  showLogModal: () => void;
  hideLogModal: () => void;
}

interface AboutModalState {
  aboutModalShown: boolean;
  showAboutModal: () => void;
  hideAboutModal: () => void;
}

export interface AppState {
  currentPack: CurrentPackState;
  map: MapTypeState;
  packsModal: PacksModalState;
  logging: LogState;
  aboutModal: AboutModalState;
}

const useAppStore = create<AppState>(set => ({
  currentPack: {
    currentPackId: null,
    setCurrentPackId: (id: string | null) => set(state => {
      // Directly updating the currentPackId property to avoid unnecessary object creation
      state.currentPack.currentPackId = id;
      return { currentPack: state.currentPack };
    }),
  },
  map: {
    mapType: 'rocks',
    setMapType: (type: MapType) => set(state => {
      // Directly updating the mapType property to avoid unnecessary object creation
      state.map.mapType = type;
      return { map: state.map };
    }),
  },
  packsModal: {
    packsModalShown: false,
    requestedPackId: null,
    showPacksModal: () => set(state => {
      // Directly updating the packsModalShown property to avoid unnecessary object creation
      state.packsModal.packsModalShown = true;
      return { packsModal: state.packsModal };
    }),
    requestPack: (packId: string) => set(state => {
      state.packsModal.packsModalShown = true;
      state.packsModal.requestedPackId = packId;
      return { packsModal: state.packsModal };
    }),
    hidePacksModal: () => set(state => {
      // Directly updating the packsModalShown property to avoid unnecessary object creation
      state.packsModal.packsModalShown = false;
      state.packsModal.requestedPackId = null;
      return { packsModal: state.packsModal };
    }),
  },
  logging: {
    log: [],
    logModalShown: false,
    addLog: (message: string) => {
      console.log(message);
      set(state => {
        state.logging.log.push({ date: new Date(), body: message });
        return { logging: state.logging };
      });
    },
    showLogModal: () => set(state => {
      state.logging.logModalShown = true;
      return { logging: state.logging };
    }),
    hideLogModal: () => set(state => {
      state.logging.logModalShown = false;
      return { logging: state.logging };
    }),
  },
  aboutModal: {
    aboutModalShown: false,
    showAboutModal: () => set(state => {
      state.aboutModal.aboutModalShown = true;
      return { aboutModal: state.aboutModal };
    }),
    hideAboutModal: () => set(state => {
      state.aboutModal.aboutModalShown = false;
      return { aboutModal: state.aboutModal };
    }),
  },
}));

// Selectors
// Since state is divided into sub-objects that don't change, I don't want to
// extract them from state b/c changes to their members will never register.
// Exporting only these selectors ensures I'm only accessing the data that I can
// react to.
export const useCurrentPackId = () => useAppStore(s => s.currentPack.currentPackId);
export const useSetCurrentPackId = () => useAppStore(s => s.currentPack.setCurrentPackId);
export const useMapType = () => useAppStore(s => s.map.mapType);
export const useSetMapType = () => useAppStore(s => s.map.setMapType);
export const usePacksModalShown = () => useAppStore(s => s.packsModal.packsModalShown);
export const useRequestedPackId = () => useAppStore(s => s.packsModal.requestedPackId);
export const useShowPacksModal = () => useAppStore(s => s.packsModal.showPacksModal);
export const useRequestPack = () => useAppStore(s => s.packsModal.requestPack);
export const useHidePacksModal = () => useAppStore(s => s.packsModal.hidePacksModal);
export const useAboutModalShown = () => useAppStore(s => s.aboutModal.aboutModalShown);
export const useShowAboutModal = () => useAppStore(s => s.aboutModal.showAboutModal);
export const useHideAboutModal = () => useAppStore(s => s.aboutModal.hideAboutModal);

// Export addLog function for use outside components
export const addLog = (message: string) => {
  useAppStore.getState().logging.addLog(message);
};

export const useLogging = () => {
  const log = useAppStore(s => s.logging.log);
  const showLogModal = useAppStore(s => s.logging.showLogModal);
  const hideLogModal = useAppStore(s => s.logging.hideLogModal);
  const logModalShown = useAppStore(s => s.logging.logModalShown);
  const addLogFn = useAppStore(s => s.logging.addLog);
  return {
    add: addLogFn,
    showLogModal,
    hideLogModal,
    log,
    logModalShown,
  };
};
