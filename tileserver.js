var Tilesplash = require('tilesplash'),
    cors = require('cors'),
    morgan = require('morgan');

// var app = new Tilesplash('postgres://kueda@localhost/underfoot', 'redis');
var app = new Tilesplash('postgres://kueda@localhost/underfoot');

// app.cache( 1000 * 60 * 5 );

// CORS support, default to allow all
app.server.use(cors());

// request logging
app.server.use(morgan('dev'));

app.layer('underfoot-units', function(tile, render){
  // console.log( "[DEBUG] tile: ", tile );
  // render('SELECT ptype, ST_AsGeoJSON(geom) as the_geom_geojson FROM units_original_4326 WHERE ST_Intersects(geom, !bbox_4326!)');
  // render('SELECT *, ST_AsGeoJSON(geom) as the_geom_geojson FROM units WHERE ST_Intersects(geom, !bbox_4326!)');
  // render('SELECT label_code, ST_AsGeoJSON(geom) as the_geom_geojson FROM units WHERE ST_Intersects(geom, !bbox!)');
  render('SELECT label_code, label_text, label_desc, rock_name, rock_type, unit, span, min_age, max_age, est_age, source, ST_AsGeoJSON( ST_Transform( geom, 4326 ) ) as the_geom_geojson FROM units WHERE ST_Intersects(geom, !bbox!)');
});

app.server.listen(8080, function() { console.log( "listening on 8080" ) });


// SELECT label_code, ST_AsGeoJSON(geom) as the_geom_geojson FROM units WHERE ST_Intersects(geom, ST_SetSRID(ST_MakeBox2D(ST_MakePoint(-13580108.193257554, 4618019.50087721), ST_MakePoint(-13540972.434775542, 4657155.25935922)), 3857));