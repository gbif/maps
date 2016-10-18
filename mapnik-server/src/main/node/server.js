var mapnik = require('mapnik')
  , mercator = require('./sphericalmercator')
  , request = require('request')
  , http = require('http')
  , url = require('url')
  , fs = require('fs')
  , yaml = require('yaml-js')
  , gbifServiceRegistry = require('./gbifServiceRegistry')
  , parser = require('./cartoParser');

/**
 * Compile the CartoCss into Mapnik stylesheets into a lookup dictionary
 */
var namedStyles = {};
namedStyles["classic.point"] = compileStylesheetSync("./cartocss/classic-dot.mss")
namedStyles["classic.poly"] = compileStylesheetSync("./cartocss/classic-poly.mss")
namedStyles["greenHeat.point"] = compileStylesheetSync("./cartocss/green-heat-dot.mss")
namedStyles["purpleYellow.point"] = compileStylesheetSync("./cartocss/purple-yellow-dot.mss")
function compileStylesheetSync(filename) {
  // snippet simulating a tilejson response from tilelive, required only to give the layers for the cartoParser
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
var defaultStyle = "classic.point";


/**
 * The server supports the ability to provide assets which need to be explicitly registered in order to be secure.
 * (e.g. trying to expose files using URL hack such as http://tiles.gbif.org/../../../hosts)
 *
 * Should this become more complex, then express or similar should be consider.
 */
var assetsHTML = ['/demo1.html', '/demo2.html', '/demo3.html', '/demo-cartodb.html']
var assertsIcon = ['/favicon.ico']

function createServer(config) {
  return http.createServer(function(req, res) {
    console.log("Request: "+req.url);

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
      parsedRequest.hostname = config.tileServer.host;
      parsedRequest.port = config.tileServer.port;
      parsedRequest.protocol = "http:";
      var tileUrl = url.format(parsedRequest);
      console.log("Fetching tile: "+tileUrl);

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
          // TODO: return 204 No Content for blank tiles, and 404 Not Found for off-the-map (beyond the projection?) tiles.
          res.writeHead(404, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
          res.end();
        }
      })

    } else {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Expected a tile URL in the form of /{map-path}/{z}/{x}/{y}.png?{params}');
    }
  });
}

/**
 * Shut down cleanly.
 */
function exitHandler() {
  console.log("Completing requests");
  server.close(function () {
    process.exit(0);
  });
}

/**
 * The main entry point.
 * Extract the configuration and start the server.  This expects a config file in YAML format and a port
 * as the only arguments.  No sanitization is performed on the file existance or content.
 */
try {
  var configFile = process.argv[2];
  var port = parseInt(process.argv[3]);
  console.log("Using config: " + configFile);
  console.log("Using port: " + port);
  var config = yaml.load(fs.readFileSync(configFile, "utf8"));
  server = createServer(config)
  server.listen(port);

  gbifServiceRegistry.register(config);

  process.on('SIGINT', exitHandler.bind());
  process.on('SIGTERM', exitHandler.bind());
  process.on('exit', exitHandler.bind());
} catch (e) {
  console.error(e);
}
