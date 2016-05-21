var fs = require('fs')
  , path = require('path')
  , path = require('path')
  , carto = require('carto');

var input = "./mb.tm2/delme.mml";
var data = JSON.parse(fs.readFileSync('./mb.tm2/delme.mml', "utf8"));

console.log(path.dirname(input));



var opts = {"name":"","description":"",
  "attribution":"",
  "center":[0,0,3],
  "format":"pbf",
  "minzoom":0,
  "maxzoom":6,
  "srs":"+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0.0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over",
  "Layer":[],
  "json":"{\"vector_layers\":[]}"};
var renderer = new carto.Renderer(null).render(opts);
console.log(renderer);

opts = {"name":"","description":"",
  "attribution":"",
  "center":[0,0,3],
  "format":"pbf",
  "minzoom":0,
  "maxzoom":6,
  "srs":"+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0.0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over",
  "Layer":[],
  "json":"{\"vector_layers\":[]}"};
renderer = new carto.Renderer(null).render(opts);
console.log(renderer);

try {

  var output = new carto.Renderer({
    filename: "delme.mml",
    local_data_dir: path.dirname(input),
  }).render(data);
  console.log(output);

} catch(err) {
  console.log(err);
}

console.log(output);
