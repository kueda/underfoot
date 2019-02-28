// Downloads DEMs from AWS-hosted Mapzen terrain tiles, converts to contours
// with gdal_contour, and loads them into PostGIS tables named contours${zoom}.
// So if you run node elevation.js 10, it will create a contours table named
// contours10.

const fs = require( "fs" );
const https = require( "https" );
const mkdirp = require( "mkdirp" );
const path = require( "path" );
const SphericalMercator = require( "@mapbox/sphericalmercator" );
const sleep = require( "sleep" ).sleep;
const exec = require( "child_process" ).exec;
const execSync = require( "child_process" ).execSync;
const _progress = require('cli-progress');
const pg = require( "pg" );

const minZoom = process.argv[2] ? parseInt( process.argv[2] ) : 10;
const maxZoom = process.argv[3] ? parseInt( process.argv[3] ) : minZoom;
const cacheDir = "elevation-tiles";
var merc = new SphericalMercator( {
  size: 256
} );
const puts = ( error, stdout, stderr ) => { console.log( stdout ) }

const tileFilePath = ( x, y, z, ext = "tif" ) => `${cacheDir}/${z}/${x}/${y}.${ext}`;

const cacheTile = ( x, y, z ) => {
  const tilePath = `${z}/${x}/${y}.tif`;
  const url = `https://s3.amazonaws.com/elevation-tiles-prod/geotiff/${tilePath}`;
  const dirPath = `./${cacheDir}/${z}/${x}`;
  const filePath = tileFilePath( x, y, z );
  return new Promise( ( resolve, reject ) => {
    // http://stackoverflow.com/questions/13696148/node-js-create-folder-or-use-existing
    mkdirp( dirPath, e => {
      if ( e ) {
        reject( Error( `Filed to write path: ${e}` ) );
      }
      if ( fs.existsSync( filePath ) ) {
        resolve( filePath );
        return;
      }
      // http://stackoverflow.com/questions/11944932/how-to-download-a-file-with-node-js-without-using-third-party-libraries
      // console.log( "Fetching ", url );
      const req = https.get( url, ( response ) => {
        if ( response.headers['content-length'] != '0') {
          // console.log( `Wrote ${filePath}` );
          const file = fs.createWriteStream( filePath, { flags: "w" } );
          response.pipe( file );
          file.on( "finish", ( ) => {
            file.close( );
            resolve( filePath );
          } );
        }
      } ).on( "error", ( e ) => {
        fs.unlink( filePath );
        reject( Error( `Failed to download ${url}, e: ${e}` ) );
      } ).on( "abort", e => {
        fs.unlink( filePath );
        reject( Error( `Client aborted download ${url}, e: ${e}` ) );
      } ).on( "aborted", e => {
        fs.unlink( filePath );
        reject( Error( `Server aborted download ${url}, e: ${e}` ) );
      } );
    } );
  } );
}

const tilesForBbox = ( swlon, swlat, nelon, nelat ) => {
  let tiles = [];
  for ( let zoom = maxZoom; zoom >= minZoom; zoom-- ) {
    let bounds = merc.xyz( [ swlon, swlat, nelon, nelat ], zoom, false );
    for ( let x = bounds.maxX + 1; x >= bounds.minX - 1; x-- ) {
      for ( let y = bounds.maxY; y >= bounds.minY; y-- ) {
        tiles.push( [x, y, zoom] );
      }
    }
  }
  return tiles;
}

const cacheTiles = ( swlon, swlat, nelon, nelat ) => {
  let tiles = tilesForBbox( swlon, swlat, nelon, nelat );
  var progressBar = new _progress.Bar( {
    format: "Caching {value}/{total} tiles [{bar}] {percentage}% | ETA: {eta_formatted}",
    etaBuffer: 1000
  } );
  progressBar.start( tiles.length, 0 );
  // Javascript: making simple things hard since forever
  let sequence = Promise.resolve( );
  let counter = 0;
  tiles.forEach( tile => {
    let [x,y,z] = tile;
    sequence = sequence.then( ( ) => {
      let promise = cacheTile( x, y, z ).then( filePath => progressBar.update( counter ) );
      counter += 1;
      return promise;
    } );
  } );
  return sequence
    .then( ( ) => progressBar.stop( ) )
    // Remove empty directories
    .then( ( ) => {
      console.log( "Removing empty directories..." )
      exec( `find ${cacheDir} -type d -empty -delete`, puts );
    } )

}

const makeContours = ( swlon, swlat, nelon, nelat ) => {
  const tiles = tilesForBbox( swlon, swlat, nelon, nelat );
  var progressBar = new _progress.Bar( {
    format: "Making {value}/{total} contours [{bar}] {percentage}% | ETA: {eta_formatted}",
    etaBuffer: 1000
  } );
  let counter = 0;
  progressBar.start( tiles.length, counter );
  tiles.forEach( tile => {
    let [x,y,z] = tile;
    const mergeContoursPath = tileFilePath( x, y, z, "merge-contours.shp" );
    if ( !fs.existsSync( mergeContoursPath ) ) {
      let interval = 1000;
      if ( z >= 10 ) {
        interval = 25;
      } else if ( z >= 8 ) {
        interval = 100;
      }
      // Merge all 8 tiles that surround this tile so we don't get weird edge
      // effects.
      const mergeCoords = [
        [x - 1, y - 1], [x + 0, y - 1], [x + 1, y - 1],
        [x - 1, y + 0], [x + 0, y + 0], [x + 1, y + 0],
        [x - 1, y + 1], [x + 0, y + 1], [x + 1, y + 1]
      ];
      const mergeFilePaths = mergeCoords.map( xy => tileFilePath( xy[0], xy[1], z ) );
      const mergePath = tileFilePath( x, y, z, "merge.tif" );
      execSync( `gdal_merge.py -q -o ${mergePath} ${mergeFilePaths.join( " " )}`, {stdio: "ignore" } );
      execSync( `gdal_contour -q -i ${interval} -a elevation ${mergePath} ${mergeContoursPath}`, {stdio: "ignore" } );
      const [e, s, w, n] = merc.bbox( x, y, z, false, "900913" );
      // Do a bunch of stuff, including clipping the lines back to the original
      // tile boundaries, projecting them into 4326, and loading them into a
      // PostGIS table named contours${zoom level}
      const ogr2ogr = `
        ogr2ogr
          -append
          -skipfailures
          -nln contours${z}
          -nlt MULTILINESTRING
          -clipsrc ${w} ${s} ${e} ${n}
          -f PostgreSQL PG:"dbname=underfoot"
          -t_srs EPSG:4326
          --config PG_USE_COPY YES
          ${mergeContoursPath}
      `.replace( /\s+/gm, " " );
      // console.log( "[DEBUG] ogr2ogr: ", ogr2ogr );
      execSync( ogr2ogr, {stdio: [0,1,2]} );
      // execSync( ogr2ogr, {stdio: "ignore" } );
      execSync( `rm -rf ${tileFilePath( x, y, z, "merge.tif" )}`, {stdio: "ignore" } );
    }
    progressBar.update( counter += 1 );
  } );
  console.log( "[DEBUG] stopping progressbar" );
  progressBar.stop( );
}

// conenct to postgres to get the extents
var pgClient = new pg.Client( { database: "underfoot", user: "underfoot", password: "underfoot" } );
pgClient.connect( err => {
  if ( err ) throw err;
  pgClient.query('SELECT ST_Extent(ST_Transform(geom, 4326)) FROM units', [], ( err, result ) => {
    if ( err ) throw err;
    matches = result.rows[0]['st_extent'].match(/BOX\(([0-9\-\.]+) ([0-9\-\.]+),([0-9\-\.]+) ([0-9\-\.]+)\)/)
    const swlat = matches[2];
    const swlon = matches[1];
    const nelat = matches[4];
    const nelon = matches[3];
    cacheTiles( swlon, swlat, nelon, nelat )
      .then( ( ) => {
        makeContours( swlon, swlat, nelon, nelat );
      } )
      .then( ( ) => {
        pgClient.end( err => {
          if ( err ) throw err;
        } );
      } );
  } );
} );
