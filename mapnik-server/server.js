var mapnik = require('mapnik')
  , mercator = require('./sphericalmercator')
  , http = require('http')
  , url = require('url')
  , fs = require('fs')
  , tilelive = require('tilelive')
  , carto = require('carto')
  , Vector = require('tilelive-vector');

// configure tilelive to a) use Mapnik and b) tilejson to find the vector tiles through config
Vector.registerProtocols(tilelive);
require('tilelive-mapnik').registerProtocols(tilelive);
require('tilejson').registerProtocols(tilelive);

var tmstyle = require('tilelive-tmstyle');

function isEmpty(obj) {
  for(var prop in obj) {
    if(obj.hasOwnProperty(prop))
      return false;
  }
  return true;
}

/*
var data =
"#layer {\n" +
  "marker-fill: #FF6600;\n" +
  "marker-opacity: 1;\n" +
  "marker-width: 16;\n" +
  "marker-line-color: white;\n" +
  "marker-line-width: 3;\n" +
  "marker-line-opacity: 0.9;\n" +
  "marker-placement: point;\n" +
  "marker-type: ellipse;\n" +
  "marker-allow-overlap: true;\n" +
"}"

// compile the carto css into the mapbox stylesheet
try {
  var output = new carto.Renderer({
    filename: "delme.xm,l",
    local_data_dir: "/tmp",
  }).renderMSS(data);
} catch(err) {
  if (Array.isArray(err)) {
    err.forEach(function(e) {
      carto.writeError(e, options);
    });
  } else { throw err; }
}
console.log(output);*/

var server = http.createServer(function(req, res) {
  console.log("Starting server");

  var query = url.parse(req.url.toLowerCase(), true).query;



  res.writeHead(500, {
    'Content-Type': 'text/plain'
  });

  if (!query || isEmpty(query)) {
    try {
      res.writeHead(200, {
        'Content-Type': 'text/html'
      });
      if (req.url == '/') {
        res.end(fs.readFileSync('./public/index.html'));
      } else {
        res.end(fs.readFileSync('./public/' + req.url));
      }
    } catch (err) {
      res.end('Not found: ' + req.url);
    }
  } else {

    if (query &&
        query.x !== undefined &&
        query.y !== undefined &&
        query.z !== undefined
    ) {

      tilelive.load('vector:///localhost:7001/api/all/0/0/0.pbf', function(err, source) {
        console.log(source);
      })


      // TODO: this sucks that we load it every request... move this out
      tilelive.load('tilejson+file:///Users/tim/dev/git/gbif/maps/mapnik-server/metadata.json?timeout=10000', function(err, source) {
        if (err) throw err;

        source.getTile(parseInt(query.z),parseInt(query.x),parseInt(query.y), function(err, tile, headers) {
          if (err) throw err;

          var map = new mapnik.Map(512, 512, mercator.proj4);
          map.bufferSize = 50;
          map.loadSync('./stylesheet2.xml');

          var vt = new mapnik.VectorTile(parseInt(query.z),parseInt(query.x),parseInt(query.y));
          vt.addDataSync(tile);
          console.log(mapnik.VectorTile.info(tile));

          vt.render(map, new mapnik.Image(512,512), function(err, image) {
            if (err) {
              res.end(err.message);
            } else {
              res.writeHead(200, {
                'Content-Type': 'image/png'
              });
              image.encode('png', function(err,buffer) {
                if (err) {
                  res.end(err.message);
                } else {
                  res.end(buffer);
                }
              });
            }
          });
        });
      })

    } else {
      res.end('missing x, y, z, sql, or style parameter');
    }
  }
});


server.listen(3000);
