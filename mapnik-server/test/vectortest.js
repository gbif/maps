var mapnik = require('mapnik')
  , mercator = require('./sphericalmercator')
  , VectorTile = require('vector-tile').VectorTile
  , Protobuf = require('pbf')
  , fs = require('fs');

var encoded = fs.readFileSync('0.pbf');
var mapnikStylesheet = fs.readFileSync('stylesheet.xml', 'utf8');

var tile = new VectorTile(new Protobuf(encoded));
console.log(tile);

console.time("setup");
var map = new mapnik.Map(512, 512, mercator.proj4);
map.fromStringSync(mapnikStylesheet); // load in the style we parsed

var vt = new mapnik.VectorTile(0,0,0);
vt.setData(fs.readFileSync('0.pbf'))
//console.log(JSON.stringify(JSON.parse(vt.toGeoJSON('__all__')),null,1))
//vt.addDataSync(encoded);

console.timeEnd("setup");

console.time("render");
vt.render(map, new mapnik.Image(512,512), {"buffer_size":5}, function(err, image) {
  if (err) {
    console.log(err.message);
  } else {
    image.encode('png', function(err,buffer) {
      if (err) {
        console.log(err.message);
      } else {
        console.timeEnd("render");
        fs.writeFile('map.png',buffer, function(err) {
          if (err) throw err;
          console.log('saved map image to map.png');
        });
      }
    });
  }
});
