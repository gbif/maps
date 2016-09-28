var mapnik = require('mapnik')
  , mercator = require('./sphericalmercator')
  , request = require('request')
  , http = require('http')
  , url = require('url')
  , fs = require('fs')
  , parser = require('./cartoParser');


/**
 * Compile the CartoCss into Mapnik stylesheets into a lookup dictionary
 */
var namedStyles = {};
namedStyles.classic = compileStylesheetSync("./cartocss/classic-dot.mss")
namedStyles.classicPoly = compileStylesheetSync("./cartocss/classic-poly.mss")
namedStyles.greenHeat = compileStylesheetSync("./cartocss/green-heat-dot.mss")
namedStyles.purpleYellow = compileStylesheetSync("./cartocss/purple-yellow-dot.mss")
function compileStylesheetSync(filename) {
  // snippet simulating a tilejson response from tilelive, only to give the layers for the cartoParser
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
  var cartocss = fs.readFileSync(filename, "utf8");
  return parser.parseToXML([cartocss], tilejson);
}
var defaultStyle = "classic";

/**
 * We support a basic diagnostic page on index.html, so handle the explicit assets to serve here.
 * We do this to avoid exposing any security holes (e.g. trying to expose files using URL hack such as
 * http://maps.gbif.org/../../../host)
 *
 * Should this become more complex, then express or simi
 */
var assetsHTML = ['/index.html', '/index2.html', '/auth.html']
var assertsIcon = ['/favicon.ico']

/**
 * TODO: Move to config
 * Defines the host and port for the vector tile server to use
 */
//var tileServerHost = "maptest-vh.gbif.org";
//var tileServerPort = "80";
var tileServerHost = "localhost";
var tileServerPort = "7001";


var server = http.createServer(function(req, res) {

  var parsedRequest = url.parse(req.url, true)

  // handle registered assets
  if (assetsHTML.indexOf(parsedRequest.path) != -1) {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end(fs.readFileSync('./public' + parsedRequest.path));

  } else if (assertsIcon.indexOf(parsedRequest.path) != -1) {
    res.writeHead(200, {'Content-Type': 'image/x-icon'});
    res.end(fs.readFileSync('./public' + parsedRequest.path));

  } else if (parsedRequest.pathname.indexOf(".png") == parsedRequest.pathname.length - 4) {

    // reformat the request to the type expected by the VectorTile Server
    parsedRequest.pathname = parsedRequest.pathname.replace("png", "mvt");
    parsedRequest.hostname = tileServerHost;
    parsedRequest.port = tileServerPort;
    parsedRequest.protocol = "http:";
    var tileUrl = url.format(parsedRequest);
    console.log(tileUrl);

    // extract the x,y,z from the URL which could be /some/map/type/{z}/{x}/{y}.mvt?srs=EPSG:4326
    var dirs = parsedRequest.pathname.substring(0, parsedRequest.pathname.length - 4).split("/");
    var x = parseInt(dirs[dirs.length - 2]);
    var y = parseInt(dirs[dirs.length - 1]);
    var z = parseInt(dirs[dirs.length - 3]);

    // find the compiled stylesheet from the given style parameter, defaulting if omitted or bogus
    var style = (parsedRequest.query.style !== undefined && parsedRequest.query.style) ?
                parsedRequest.query.style : defaultStyle;
    var stylesheet = namedStyles[style];
    stylesheet = (stylesheet !== undefined && stylesheet) ? stylesheet : namedStyles[defaultStyle];

    // issue the request to the vector tile server and render the tile as a PNG using Mapnik
    console.time("getTile");
    request.get({url: tileUrl, method: 'GET', encoding: null}, function (error, response, body) {

      if (!error && response.statusCode == 200 && body.length > 0) {
        console.timeEnd("getTile");

        var map = new mapnik.Map(512, 512, mercator.proj4);
        map.fromStringSync(stylesheet);
        var vt = new mapnik.VectorTile(z, x, y);
        vt.addDataSync(body);

        // important to include a buffer, to catch the overlaps
        console.time("render");
        vt.render(map, new mapnik.Image(512, 512), {"buffer_size": 8}, function (err, image) {
          if (err) {
            res.end(err.message);
          } else {
            res.writeHead(200, {
              'Content-Type': 'image/png',
              'Access-Control-Allow-Origin': '*',
              'Cache-Control': 'max-age=600, must-revalidate'    // TODO: configurify the cache control!
            });
            console.timeEnd("render");
            image.encode('png', function (err, buffer) {

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
        res.writeHead(404, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
        res.end();
      }
    })

  } else {
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Expected a tile URL in the form of /{map-path}/{z}/{x}/{y}.png?{params}');
  }
});

server.listen(3000);  // TODO: configurify the port!
