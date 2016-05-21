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

TMSource = require('tilelive-tmsource')(tilelive);

function isEmpty(obj) {
  for(var prop in obj) {
    if(obj.hasOwnProperty(prop))
      return false;
  }
  return true;
}


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

      //tilelive.load('tmsource://./mb.tm2', function(err, source) {
      tilelive.load('tilejson+http://localhost:3000/metadata3.json', function(err, source) {

        /*
        source.getInfo(source, function(err, info) {
          if (err) throw err;
          console.log(JSON.stringify(info));
        });
        */

        if (err) throw err;
        console.log(JSON.stringify(source));



        source.getTile(parseInt(query.z),parseInt(query.x),parseInt(query.y), function(err, tile, headers) {
          if (err) {
            console.log(err);
            return;
          }
          console.log(tile);

          var map = new mapnik.Map(512, 512);
          //map.bufferSize(40);
          //map.loadSync('./stylesheet.xml');

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
