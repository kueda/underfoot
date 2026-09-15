import { startCase } from 'lodash';

import { RockUnit } from '../../packs/types';
import { humanizeAge } from './util';

// Header for rock unit in MapBottomSheet
export default function RocksHeader({ feature }: { feature?: RockUnit }) {
  return (
    <>
      <h3>
        {feature?.title || 'Unknown'}
      </h3>
      <div className="MapBottomSheetHeaderPreview">
        <div>
          <label>Lithology</label>
          {startCase(feature?.lithology || 'Unknown')}
        </div>
        <div>
          <label>Age</label>
          {feature?.est_age ? humanizeAge(feature.est_age) : 'Unknown'}
        </div>
      </div>
    </>
  );
}
