import { startCase } from 'lodash';
import autolinker from 'autolinker';

import { RockUnit } from '../../packs/types';
import { humanizeAge } from './util';

export default function RocksBody({ feature, descRef }: {
  feature?: RockUnit;
  descRef: React.RefObject<HTMLParagraphElement>;
}) {
  return (
    <>
      { feature?.description && (
        <>
          <h3>Description</h3>
          <p ref={descRef}>{feature?.description}</p>
        </>
      ) }
      <h3>Estimated Age</h3>
      <p>
        {startCase(feature?.controlled_span)}
        {' '}
        (
        {humanizeAge(feature?.min_age)}
        {' '}
        -
        {' '}
        {humanizeAge(feature?.max_age)}
        )
      </p>
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
