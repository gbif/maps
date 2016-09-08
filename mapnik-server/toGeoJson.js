var mapnik = require('mapnik')
var fs = require('fs');

var vt = new mapnik.VectorTile(3,6,1);
vt.addDataSync(fs.readFileSync('/tmp/ebird.mvt'));
//vt.parse()
console.log(JSON.stringify(JSON.parse(vt.toGeoJSON('__all__')),null,1))
