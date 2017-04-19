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
namedStyles["green.poly"] = compileStylesheetSync("./cartocss/green-poly.mss")
namedStyles["green2.poly"] = compileStylesheetSync("./cartocss/green2-poly.mss")
namedStyles["orange.marker"] = compileStylesheetSync("./cartocss/orange-marker.mss")
namedStyles["blue.marker"] = compileStylesheetSync("./cartocss/blue-marker.mss")
namedStyles["outline.poly"] = compileStylesheetSync("./cartocss/outline-poly.mss")
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
 * (e.g. trying to expose files using URL hack such as http://api.gbif.org/v1/map/../../../hosts)
 *
 * Should this become more complex, then express or similar should be consider.
 */
var assetsHTML = [
  '/map/demo.html',
  '/map/demo1.html',
  '/map/demo2.html',
  '/map/demo3.html',
  '/map/demo4.html',
  '/map/demo5.html',
  '/map/demo6.html',
  '/map/demo7.html',
  '/map/demo8.html',
  '/map/demo-cartodb.html'];

function parseUrl(parsedRequest) {
  if (parsedRequest.pathname.endsWith(".png")) {

    // extract the x,y,z from the URL which could be /some/map/type/{z}/{x}/{y}@{n}x.png?srs=EPSG:4326&...
    var dirs = parsedRequest.pathname.substring(0, parsedRequest.pathname.length - 7).split("/");
    var z = parseInt(dirs[dirs.length - 3]);
    var x = parseInt(dirs[dirs.length - 2]);
    var y = parseInt(dirs[dirs.length - 1]);

    // find the compiled stylesheet from the given style parameter, defaulting if omitted or bogus
    var stylesheet = (parsedRequest.query.style in namedStyles)
      ? namedStyles[parsedRequest.query.style]
      : namedStyles[defaultStyle];

    var density = parseInt(parsedRequest.pathname.substring(parsedRequest.pathname.length - 6, parsedRequest.pathname.length - 5));

    if (!(isNaN(z) || isNaN(x) || isNaN(y) || isNaN(density))) {
      return {
        "z": z,
        "x": x,
        "y": y,
        "density": density,
        "stylesheet": stylesheet
      }
    }
  }
  throw Error("URL structure is invalid, expected /some/map/type/{z}/{x}/{y}@{n}x.png?srs=EPSG:4326&...");
}

function vectorRequest(parsedRequest) {
  // reformat the request to the type expected by the VectorTile Server
  // Remove raster parameters, to improve cacheability
  delete parsedRequest.query.style;
  delete parsedRequest.search; // Must be removed to force regeneration of query string
  parsedRequest.pathname = parsedRequest.pathname.replace("@1x.png", ".mvt");
  parsedRequest.pathname = parsedRequest.pathname.replace("@2x.png", ".mvt");
  parsedRequest.pathname = parsedRequest.pathname.replace("@3x.png", ".mvt");
  parsedRequest.pathname = parsedRequest.pathname.replace("@4x.png", ".mvt");
  parsedRequest.hostname = config.tileServer.host;
  parsedRequest.port = config.tileServer.port;
  parsedRequest.pathname = config.tileServer.prefix + parsedRequest.pathname;
  parsedRequest.protocol = "http:";
  return url.format(parsedRequest);
}

function createServer(config) {
  return http.createServer(function(req, res) {
    console.log("Request:", req.url);

    var parsedRequest = url.parse(req.url, true)

    // handle registered assets
    if (assetsHTML.indexOf(parsedRequest.path) != -1) {
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.end(fs.readFileSync('./public' + parsedRequest.path));

    } else {
      // Handle map tiles.

      try {
        var parameters = parseUrl(parsedRequest);
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end(e.message);
        return;
      }

      var vectorTileUrl = vectorRequest(parsedRequest);

      // issue the request to the vector tile server and render the tile as a PNG using Mapnik
      console.log("Fetching vector tile:", vectorTileUrl);
      //console.time("getTile");
      request.get({url: vectorTileUrl, method: 'GET', encoding: null}, function (error, response, body) {

        if (!error) {
          console.log("Vector tile has HTTP status", response.statusCode, "and size", body.length);

          if (response.statusCode == 200 && body.length > 0) {
            //console.timeEnd("getTile");

            var size = 512 * parameters.density;

            try {
              var map = new mapnik.Map(size, size, mercator.proj4);
              map.fromStringSync(parameters.stylesheet);
              // Pretend it's tile 0, 0, since Mapnik validates the address according to the standard Google schema,
              // and we aren't using it for WGS84.
              var vt = new mapnik.VectorTile(parameters.z, 0, 0);
              vt.addDataSync(body);

              // important to include a buffer, to catch the overlaps
              //console.time("render");
              vt.render(map, new mapnik.Image(size, size), {
                "buffer_size": 8,
                "scale": parameters.density
              }, function (err, image) {
                if (err) {
                  res.end(err.message);
                } else {
                  res.writeHead(200, {
                    'Content-Type': 'image/png',
                    'Access-Control-Allow-Origin': '*',
                    'Cache-Control': 'max-age=600, must-revalidate'    // TODO: configure the cache control!
                  });
                  //console.timeEnd("render");
                  image.encode('png', function (err, buffer) {

                    if (err) {
                      res.end(err.message);
                    } else {
                      res.end(buffer);
                    }
                  });
                }
              });
            } catch (e) {
              // something went wrong
              res.writeHead(500, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
              res.end(e.message);
              console.log(e);
            }

          } else {
            if (response.statusCode == 404 ||   // not found
              response.statusCode == 204 ||   // no content
              (response.statusCode == 200 && body.length == 0)) {  // accepted but no content
              // no tile
              res.writeHead((response.statusCode == 200) ? 204 : response.statusCode, // keep same status code
                {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
              res.end();
            } else {
              res.writeHead(500, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
              res.end();
            }
          }

        } else { // error
          // something went wrong
          console.log("Error retrieving vector tile");
          res.writeHead(503, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
          res.end();
        }
      })
    }
  });
}

/**
 * Shut down cleanly.
 */
function exitHandler() {
  console.log("Completing requests");
  // Until https://github.com/nodejs/node/issues/2642 is fixed, we can't wait for connections to end.
  //server.close(function () {
    process.exit(0);
  //});
}

/**
 * The main entry point.
 * Extract the configuration and start the server.  This expects a config file in YAML format and a port
 * as the only arguments.  No sanitization is performed on the file existence or content.
 */
try {
  process.on('SIGHUP', () => {console.log("Ignoring SIGHUP")});

  // Log if we crash.
  process.on('uncaughtException', function (exception) {
    console.trace(exception);
    exitHandler();
  });
  process.on('unhandledRejection', (reason, p) => {
    console.log("Unhandled Rejection at: Promise ", p, " reason: ", reason);
    exitHandler();
  });

  // Set up server.
  var configFile = process.argv[2];
  var port = parseInt(process.argv[3]);
  console.log("Using config: " + configFile);
  console.log("Using port: " + port);
  var config = yaml.load(fs.readFileSync(configFile, "utf8"));
  var server = createServer(config)
  server.listen(port);

  // Set up ZooKeeper.
  gbifServiceRegistry.register(config);

  // Aim to exit cleanly.
  process.on('SIGINT', exitHandler.bind());
  process.on('SIGTERM', exitHandler.bind());
  process.on('exit', exitHandler.bind());
} catch (e) {
  console.error(e);
}
