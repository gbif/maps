var mapnik = require('mapnik')
  , mercator = require('./sphericalmercator')
  , request = require('request')
  , async = require('async')
  , http = require('http')
  , url = require('url')
  , fs = require('fs')
  , tilelive = require('tilelive')
  , carto = require('carto')
  , parser = require('./cartoParser');



// register fonts and datasource plugins
mapnik.register_default_fonts();
mapnik.register_default_input_plugins();

var vtile = new mapnik.VectorTile(0,0,0);
vtile.setDataSync(fs.readFileSync('/tmp/0.mvt'))
//vtile.parse()

vtile.toGeoJSON("occurrence", function(err, geojson) {
  if (err) throw err;
  //console.log(geojson); // stringified GeoJSON
  //console.log(JSON.parse(geojson)); // GeoJSON object
});



var map = new mapnik.Map(512, 512);
//map.load('stylesheet-hot.xml', function(err,map) {
//map.load('stylesheet-hot-4326.xml', function(err,map) {  // seems unnecessary
map.load('stylesheet2.xml', function(err,map) {
  if (err) throw err;
  map.zoomAll();

  vtile.render(map, new mapnik.Image(512,512), {"buffer_size":5}, function(err, im) {
    if (err) throw err;
    im.encode('png', function(err,buffer) {
      if (err) throw err;
      fs.writeFile('map.png',buffer, function(err) {
        if (err) throw err;
        console.log('saved map image to map.png');
      });
    });

  });

});
