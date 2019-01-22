/**
  *
  * Trivial script for generating underfoot tiles from a tile server. Assumes
  * endpoints like http://localhost:8080/underfoot-units/${z}/${x}/${y}.mvt are
  * available.
  *
  * Usage:
  * # Generate tiles for zoom levels 7 to 12:
  * node cachetiles.js 7 12
  *
 **/

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
const tilelive = require( "@mapbox/tilelive" );
const MBTiles = require( "@mapbox/mbtiles" );
const Omnivore = require( "@mapbox/tilelive-omnivore" );

Omnivore.registerProtocols( tilelive );
MBTiles.registerProtocols( tilelive );

const minZoom = process.argv[2] ? parseInt( process.argv[2] ) : 10;
const maxZoom = process.argv[3] ? parseInt( process.argv[3] ) : 13;
const cacheDir = "elevation-tiles";
var merc = new SphericalMercator( {
  size: 256
} );
const puts = ( error, stdout, stderr ) => { console.log( stdout ) }

const cacheTile = ( x, y, z ) => {
  const tilePath = `${z}/${x}/${y}.tif`;
  const url = `https://s3.amazonaws.com/elevation-tiles-prod/geotiff/${tilePath}`;
  const dirPath = `./${cacheDir}/${z}`;
  const filePath = `${dirPath}/${x}-${y}.tif`
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

const cacheTiles = ( swlon, swlat, nelon, nelat ) => {
  let tiles = [];
  for ( let zoom = maxZoom; zoom >= minZoom; zoom-- ) {
    let bounds = merc.xyz( [ swlon, swlat, nelon, nelat ], zoom, false );
    for ( let x = bounds.maxX + 1; x >= bounds.minX - 1; x-- ) {
      for ( let y = bounds.maxY; y >= bounds.minY; y-- ) {
        tiles.push( [x, y, zoom] );
      }
    }
  }
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

const makeContours = ( ) => {
  console.log( "Making contours" )
  for ( let zoom = maxZoom; zoom >= minZoom; zoom-- ) {
    let interval = 1000;
    if ( zoom >= 12 ) {
      interval = 10;
    } else if ( zoom >= 10 ) {
      interval = 100;
    }
    console.log( `Zoom ${zoom}, interval ${interval}, merge...` )
    execSync( `gdal_merge.py -o elevation-tiles/${zoom}.tif ${cacheDir}/${zoom}/*.tif`, {stdio: [0,1,2]} );
    console.log( `Zoom ${zoom}, interval ${interval}, contours...` )
    execSync( `gdal_contour -i ${interval} -a elevation ${cacheDir}/${zoom}.tif ${cacheDir}/contours-${zoom}.shp`, {stdio: [0,1,2]} );
  }
}

const makeMBTiles = ( z ) => {
  const outURI = `mbtiles://${__dirname}/elevation-tiles.mbtiles`;
  // for ( let zoom = maxZoom; zoom >= minZoom; zoom-- ) {
  console.log( "[DEBUG] minZoom: ", minZoom );
  console.log( "[DEBUG] maxZoom: ", maxZoom );
  let zoom = parseInt( z, 0 ) || minZoom;
  zoom = zoom < minZoom ? minZoom : zoom;
  console.log( "[DEBUG] zoom: ", zoom );
  if ( zoom > maxZoom ) {
    return;
  }
  const inpURI = `omnivore://${__dirname}/${cacheDir}/contours-${zoom}.shp`;
  tilelive.load( inpURI, ( err, inp ) => {
    if (err) {
      Error( `err loading inp: ${err}` );
    }
    tilelive.load( outURI, ( err, out ) => {
      if (err) {
        Error( `err loading out: ${err}` );
      }
      var options = {
        minzoom: zoom,
        maxzoom: zoom,
        listScheme: out.createZXYStream( )
      };
      tilelive.copy( inp, out, options, err => {
        console.log( `Done writing zoom ${zoom} to mbtiles` );
        makeMBTiles( zoom + 1 );
      } );
    } );
  } );
}

// conenct to postgres to get the extents
var pgClient = new pg.Client( { database: "underfoot", password: "vagrant" } );
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
        makeContours( );
      } )
      .then( ( ) => {
        makeMBTiles( );
      } );
    pgClient.end( err => {
      if ( err ) throw err;
    } );
  } );
} );
