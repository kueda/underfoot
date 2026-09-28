import { WaterFeature } from '../../packs/types';

export default function WaterHeader({ feature }: { feature?: WaterFeature }) {
  let displayLayer = 'Unknown';
  switch (feature?.layer) {
    case 'waterways':
      displayLayer = 'Waterway';
      break;
    case 'waterbodies':
      displayLayer = 'Waterbody';
      break;
    case 'watersheds':
      displayLayer = 'Watershed';
      break;
  }
  const permanence = feature?.permanence;
  if (permanence) {
    const capitalized = `${permanence[0].toUpperCase()}${permanence.slice(1)}`;
    displayLayer = `${capitalized} ${displayLayer.toLowerCase()}`;
  }
  return (
    <>
      <h3>
        {feature?.title || 'Unknown'}
      </h3>
      <div className="MapBottomSheetHeaderPreview">
        <div>{displayLayer}</div>
      </div>
    </>
  );
}
