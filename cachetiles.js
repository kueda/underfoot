// had to do http://stackoverflow.com/questions/33604470/unexpected-token-import-in-nodejs5-and-babel#33608835 to get es6 to work
// run with babel-node cachetiles.js
// import fs from "fs";
// import http from "http";
// import mkdirp from "mkdirp";
// import path from "path";
// import SphericalMercator from "sphericalmercator";
// import { sleep } from "sleep"
const fs = require( "fs" );
const http = require( "http" );
const mkdirp = require( "mkdirp" );
const path = require( "path" );
const SphericalMercator = require( "sphericalmercator" );
const sleep = require( "sleep" ).sleep;
// const sys = require( "sys" );
const exec = require( "child_process" ).exec;
const _progress = require('cli-progress');

// psql underfoot -c "COPY ( select ST_AsGeoJSON(ST_Extent(geom)) from units_original_4326 ) TO STDOUT" > underfoot_units-extent.json 
// const geojson = JSON.parse( fs.readFileSync( "./underfoot_units-extent.json" ) );
// const limits  = {
//   min_zoom: 7,
//   max_zoom: 15
// };
const minZoom = 7;
const maxZoom = 15;
// const maxZoom = 11;
const swlat = 36.9192;
const swlon = -123.626;
const nelat = 38.882;
const nelon = -121.1957;
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

let tiles = [];
for ( let zoom = maxZoom; zoom >= minZoom; zoom-- ) {
  let bounds = merc.xyz( [ swlon, swlat, nelon, nelat], zoom, false );
  for ( let x = bounds.maxX; x >= bounds.minX; x-- ) {
    for ( let y = bounds.maxY; y >= bounds.minY; y-- ) {
      tiles.push( [x, y, zoom] );
    }
  }
}

var progressBar = new _progress.Bar( {
  format: "Caching {value}/{total} tiles [{bar}] {percentage}% | ETA: {eta_formatted}",
  etaBuffer: 100
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
