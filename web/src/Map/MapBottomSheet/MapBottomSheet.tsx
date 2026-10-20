import { Drawer as Vaul } from 'vaul';
import { useRef, useState } from 'react';
import IconButton from '@mui/material/IconButton';
import CloseFullscreenIcon from '@mui/icons-material/CloseFullscreen';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import type { Map } from 'maplibre-gl';

import type { RockUnit, UnderfootFeature, WaterFeature } from '../../packs/types';
import RocksHeader from './RocksHeader';
import RocksBody from './RocksBody';
import WaterHeader from './WaterHeader';
import WaterBody from './WaterBody';
import OsmEditLink from './OsmEditLink';

interface Props {
  feature?: UnderfootFeature | RockUnit | WaterFeature;
  mapType: 'rocks' | 'water';
  map?: Map;
}

const CLOSED_HEIGHT = '90px';

function MapBottomSheet({ feature, mapType, map }: Props) {
  const [snap, setSnap] = useState<number | string | null | undefined>(CLOSED_HEIGHT);
  const bodyRef = useRef<HTMLDivElement>(null);
  const descRef = useRef<HTMLParagraphElement>(null);

  // If the description is more than twice the height of the body, treat it as
  // scrollable. Allowing scrolling messes up the drag to dismiss
  // interaction, so it should be avoided when we don't have to scroll to
  // show content
  const scrollable = (descRef?.current?.clientHeight || 0) >= (bodyRef?.current?.clientHeight || 0) / 2;

  // The rocks map also shows the sea from the water tiles
  const isRockUnit = mapType === 'rocks' && !(feature && 'layer' in feature);

  return (
    <Vaul.Root
      open
      snapPoints={[CLOSED_HEIGHT, 1]}
      activeSnapPoint={snap}
      setActiveSnapPoint={setSnap}
      dismissible={false}
      modal={false}
    >
      <Vaul.Portal>
        <Vaul.Content
          style={{
            position: 'fixed',
            width: '100%',
            height: '100%',
            bottom: 0,
            left: 0,
            right: 0,
            // the rest is gravy
            backgroundColor: 'rgba(255, 255, 255, 0.95)',
            zIndex: 1101,
          }}
        >
          <div className={`MapBottomSheet ${snap === 1 ? 'open' : ''} ${scrollable ? 'scrollable' : ''}`}>
            <div
              className="MapBottomSheetHeader"
              style={{ height: CLOSED_HEIGHT }}
            >
              <div className="MapBottomSheetHeaderContent">
                { isRockUnit
                  ? <RocksHeader feature={feature as RockUnit} />
                  : <WaterHeader feature={feature as WaterFeature} />}
              </div>
              <div className="MapBottomSheetHeaderActions">
                <IconButton
                  aria-label="show full feature properties"
                  onClick={() => setSnap(snap === 1 ? CLOSED_HEIGHT : 1)}
                >
                  { snap === 1
                    ? <CloseFullscreenIcon />
                    : <OpenInFullIcon />}
                </IconButton>
              </div>
            </div>
            <div className="MapBottomSheetBody" ref={bodyRef}>
              { isRockUnit
                ? <RocksBody feature={feature as RockUnit} descRef={descRef} />
                : <WaterBody feature={feature as WaterFeature} />}
              <OsmEditLink feature={feature} map={map} />
            </div>
          </div>
        </Vaul.Content>
      </Vaul.Portal>
    </Vaul.Root>
  );
}

export default MapBottomSheet;
