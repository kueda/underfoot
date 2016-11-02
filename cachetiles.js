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
const http = require( "http" );
const mkdirp = require( "mkdirp" );
const path = require( "path" );
const SphericalMercator = require( "sphericalmercator" );
const sleep = require( "sleep" ).sleep;
const exec = require( "child_process" ).exec;
const _progress = require('cli-progress');
const pg = require( "pg" );

const minZoom = process.argv[2] ? parseInt( process.argv[2] ) : 7;
const maxZoom = process.argv[3] ? parseInt( process.argv[3] ) : 14;
const cacheDir = "UnderfootApp/www/tiles";
var merc = new SphericalMercator( {
  size: 256
} );

const cacheTile = ( x, y, z ) => {
  const tilePath = `underfoot-units/${z}/${x}/${y}.mvt`;
  const url = `http://localhost:8080/${tilePath}`;
  const filePath = `./${cacheDir}/${tilePath}`;
  const dirPath = path.dirname( filePath );
  return new Promise( ( resolve, reject ) => {
    // http://stackoverflow.com/questions/13696148/node-js-create-folder-or-use-existing
    mkdirp( dirPath, e => {
      if ( e ) {
        reject( Error( `Filed to write path: ${e}` ) );
      }
      // http://stackoverflow.com/questions/11944932/how-to-download-a-file-with-node-js-without-using-third-party-libraries
      // console.log( "Fetching ", url );
      const req = http.get( url, ( response ) => {
        if ( response.headers['content-length'] != '0') {
          // console.log( `Wrote ${filePath}` );
          const file = fs.createWriteStream( filePath, { flags: "w" } );
          response.pipe( file );
          file.on( "finish", ( ) => file.close() );
        }
        resolve( filePath );
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
    for ( let x = bounds.maxX; x >= bounds.minX; x-- ) {
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
  sequence.then( ( ) => progressBar.stop( ) );

  // Remove empty directories
  sequence = sequence.then( ( ) => {
    function puts( error, stdout, stderr ) { console.log( stdout ) }
    exec( `find ${cacheDir} -type d -empty -delete`, puts );
  });
}

// conenct to postgres to get the extents
var pgClient = new pg.Client( { database: "underfoot" } );
pgClient.connect( err => {
  if ( err ) throw err;
  pgClient.query('SELECT ST_Extent(ST_Transform(geom, 4326)) FROM units', [], ( err, result ) => {
    if ( err ) throw err;
    matches = result.rows[0]['st_extent'].match(/BOX\(([0-9\-\.]+) ([0-9\-\.]+),([0-9\-\.]+) ([0-9\-\.]+)\)/)
    const swlat = matches[2];
    const swlon = matches[1];
    const nelat = matches[4];
    const nelon = matches[3];
    cacheTiles( swlon, swlat, nelon, nelat );
    pgClient.end( err => {
      if ( err ) throw err;
    } );
  } );
} );
