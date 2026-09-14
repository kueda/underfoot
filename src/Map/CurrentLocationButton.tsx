import Fab from '@mui/material/Fab';
import GpsFixedIcon from '@mui/icons-material/GpsFixed';
import GpsNotFixedIcon from '@mui/icons-material/GpsNotFixed';
import CircularProgress from '@mui/material/CircularProgress';
import { ThemeProvider, createTheme, useTheme } from '@mui/material/styles';
import { Map, Marker } from 'maplibre-gl';
import { useEffect, useRef, useState } from 'react';

// https://devcodef1.com/news/1107627/custom-color-palette-in-material-ui
declare module '@mui/material/styles' {
  interface Palette {
    white: Palette['primary'];
  }

  interface PaletteOptions {
    white?: PaletteOptions['primary'];
  }
}

declare module '@mui/material/CircularProgress' {
  interface CircularProgressPropsColorOverrides {
    white: true;
  }
}

const customPalette = {
  white: {
    main: '#ffffff',
  },
};

// TODO pull this theme out somewhere more general
const customTheme = createTheme({
  palette: {
    ...customPalette,
  },
});

interface Props {
  map?: Map;
}

const CurrentLocationButton = ({ map }: Props) => {
  const [marker, setMarker] = useState<Marker | null>(null);
  const [watchId, setWatchId] = useState<number>();
  const [position, setPosition] = useState<GeolocationPosition>();
  const isTracking = useRef(false);
  const theme = useTheme();
  const loading = typeof (watchId) === 'number' && !position;

  useEffect(() => {
    if (!map) return;
    if (!position) return;
    if (marker) {
      marker.setLngLat({
        lat: position.coords.latitude,
        lng: position.coords.longitude,
      });
      return;
    }
    const element = document.createElement('div');
    element.style.width = '15px';
    element.style.height = '15px';
    element.style.borderRadius = '15px';
    element.style.backgroundColor = theme.palette.primary.main;
    element.style.border = '2px solid white';
    element.style.boxShadow = '0 0 5px black';
    const newMarker = new Marker({ element }).setLngLat({
      lat: position.coords.latitude,
      lng: position.coords.longitude,
    }).addTo(map);
    setMarker(newMarker);
    map.panTo([
      position.coords.longitude,
      position.coords.latitude,
    ]);
  }, [
    marker,
    map,
    position,
    theme,
  ]);

  useEffect(() => {
    if (isTracking.current && position) {
      map?.panTo({
        lat: position.coords.latitude,
        lng: position.coords.longitude,
      });
    }
  }, [isTracking, position, map]);

  useEffect(() => {
    map?.on('moveend', moveEvent => {
      if (moveEvent.originalEvent) {
        isTracking.current = false;
      }
    });
  }, [map]);

  let icon = <GpsNotFixedIcon />;
  if (loading) icon = <CircularProgress color="white" size={25} thickness={5} />;
  else if (isTracking.current) icon = <GpsFixedIcon />;

  return (
    <ThemeProvider theme={customTheme}>
      <Fab
        color="primary"
        aria-label="Current location"
        style={{ position: 'absolute', right: 15, bottom: 105 }}
        onClick={() => {
          isTracking.current = true;
          if (position) {
            map?.panTo([
              position.coords.longitude,
              position.coords.latitude,
            ]);
          }
          if (!watchId) {
            const newWatchId = navigator.geolocation.watchPosition(
              (position: GeolocationPosition) => setPosition(existing => {
                if (
                  existing?.coords.latitude === position.coords.latitude
                  && existing?.coords.longitude === position.coords.longitude
                ) return existing;
                return position;
              }),
              (error: GeolocationPositionError) => {
                console.error('[CurrentLocationButton.tsx] ', error);
                setWatchId(undefined);
              },
              { enableHighAccuracy: true },
            );
            setWatchId(newWatchId);
          }
        }}
      >
        {icon}
      </Fab>
    </ThemeProvider>
  );
};

export default CurrentLocationButton;
