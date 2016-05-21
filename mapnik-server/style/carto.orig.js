var mapnik = require('mapnik')
  , http = require('http')
  , url = require('url')
  , fs = require('fs')
  , tilelive = require('tilelive')
  , carto = require('carto')
  , Vector = require('tilelive-vector');


var data = fs.readFileSync('style.mss','utf8')
//console.log(data)

// compile the carto css into the mapbox stylesheet
try {
  var output = new carto.Renderer({
    filename: "delme.mml",
    local_data_dir: "/tmp",
  }).renderMSS(data);
} catch(err) {
  console.log(err);
}
console.log(output);
