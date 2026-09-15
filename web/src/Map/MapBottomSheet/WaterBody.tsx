import autolinker from 'autolinker';

import { WaterFeature } from '../../packs/types';

export default function WaterBody({ feature }: {
  feature?: WaterFeature;
}) {
  return (
    <>
      <h4>Source</h4>
      <small
        dangerouslySetInnerHTML={{
          __html: feature?.citation
            ? autolinker.link(feature.citation)
            : feature?.source || '',
        }}
      />
    </>
  );
}
