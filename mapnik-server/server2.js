var mapnik = require('mapnik')
  , mercator = require('./sphericalmercator')
  , request = require('request')
  , async = require('async')
  , http = require('http')
  , url = require('url')
  , fs = require('fs')
  , parser = require('./cartoParser');

function isEmpty(obj) {
  for(var prop in obj) {
    if(obj.hasOwnProperty(prop))
      return false;
  }
  return true;
}

// snippet simulating tilejson response from tilelive, only to give the layers
var tilejson = {
  data: {
    "vector_layers": [
      {
        "id": "occurrence",
        "description": "The GBIF occurrence data"
      }
    ]
  }
}

// load our stylesheet from cartocss and convert them to Mapnik XML stylesheet
var cartocss = fs.readFileSync("public/gbif-classic.mss", "utf8");
//var cartocss = fs.readFileSync("public/gbif-hot3.mss", "utf8");
//var cartocss = fs.readFileSync("public/gbif-various.mss", "utf8");
var stylesheet = parser.parseToXML([cartocss], tilejson);

var server = http.createServer(function(req, res) {

  var query = url.parse(req.url.toLowerCase(), true).query;

  if (!query || isEmpty(query)) {
    try {
      res.writeHead(200, {
        'Content-Type': 'text/html'
      });

      // support a basic asset server
      if (req.url == '/') {
        res.end(fs.readFileSync('./public/index.html'));
      } else {
        res.end(fs.readFileSync('./public/' + req.url));
      }
    } catch (err) {
      res.writeHead(500, {
        'Content-Type': 'text/plain'
      });
      res.end('Not found: ' + req.url);
    }
  } else {

    if (query &&
        query.x !== undefined &&
        query.y !== undefined &&
        query.z !== undefined
    ) {
      console.time("getTile");

      var x = parseInt(query.x);
      var y = parseInt(query.y);
      var z = parseInt(query.z);

      //var tileUrl = "http://localhost/api/occurrence/density/" + z + "/" + x + "/" + y + ".mvt?taxonKey=5231190";
      var tileUrl = "http://localhost/api/occurrence/density/" + z + "/" + x + "/" + y + ".mvt";


      request.get({url: tileUrl, method: 'GET', encoding: null}, function (error, response, body) {
        if (!error && response.statusCode == 200 && body.length>0) {
          console.timeEnd("getTile");

          console.time("setup");
          var map = new mapnik.Map(512, 512, mercator.proj4);
          map.fromStringSync(stylesheet);
          var vt = new mapnik.VectorTile(z,x,y);

          vt.addDataSync(body);
          console.timeEnd("setup");

          // important to include a buffer, to catch the overlaps
          console.time("render");
          vt.render(map, new mapnik.Image(512,512), {"buffer_size":25}, function(err, image) {
            if (err) {
              res.end(err.message);
            } else {
              res.writeHead(200, {
                'Content-Type': 'image/png'
              });
              console.timeEnd("render");
              image.encode('png', function(err,buffer) {

                if (err) {
                  res.end(err.message);
                } else {
                  res.end(buffer);
                }
              });
            }
          });
        } else {
          // no tile
          res.writeHead(404, {
            'Content-Type': 'image/png'
          });
          res.end();
        }
      })

    } else {
      res.writeHead(500, {
        'Content-Type': 'text/plain'
      });
      res.end('missing x, y, z, sql, or style parameter');
    }
  }
});

server.listen(3000);
