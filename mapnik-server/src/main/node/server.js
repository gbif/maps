const request = require('request')
    , http = require('http')
    , fs = require('fs')
    , yaml = require('yaml-js')
    , gbifServiceRegistry = require('./gbifServiceRegistry')
    , routes = require('./routes')
    , renderer = require('./renderer');

function createServer(config) {
  return http.createServer(function(req, res) {
    console.log("Request:", req.url);

    var x = routes(req, res);
    var parameters = x.parameters;
    var vectorTileUrl = x.vectorTileUrl;

    // issue the request to the vector tile server and render the tile as a PNG using Mapnik
    console.log("Fetching vector tile:", vectorTileUrl);
    console.time("getTile");
    request.get({url: vectorTileUrl, method: 'GET', encoding: null, gzip: true}, function (error, response, body) {
      console.timeEnd("getTile");

      if (error) {
        // something went wrong
        console.log("Error retrieving vector tile");
        res.writeHead(503, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
        res.end(error); // TODO: put this in headers instead
      }

      console.log("Vector tile has HTTP status", response.statusCode, "and size", body.length);

      // Pass along an ETag if present, to aid caching.
      // (NB reliant on Varnish to handle If-None-Match request, we always return 200 or 204.)
      if (response.headers['etag']) {
        etag = response.headers['etag'];
      } else {
        etag = '"' + Date.now() + '"';
      }

      if (response.statusCode == 200 && body.length > 0) {
        res.writeHead(200, {
          'Content-Type': 'image/png',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=600',
          'ETag': etag,
        });

        try {
          renderer(parameters, body, res);
        } catch (e) {
          // something went wrong
          res.writeHead(500, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
          res.end(e.message);
          console.log(e);
        }

      } else if (response.statusCode == 404 ||   // not found
                 response.statusCode == 204 ||   // no content
                 (response.statusCode == 200 && body.length == 0)) {  // accepted but no content
        // no tile
        res.writeHead((response.statusCode == 200) ? 204 : response.statusCode, // keep same status code, except empty 200s.
                      {
                        'Content-Type': 'image/png',
                        'Access-Control-Allow-Origin': '*',
                        'Cache-Control': 'public, max-age=600',
                        'ETag': etag,
                      });
        res.end();
      } else {
        res.writeHead(500, {'Content-Type': 'image/png'}); // type only for ease of use with e.g. leaflet
        res.end();
      }
    })
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
//try {
//  process.on('SIGHUP', () => {console.log("Ignoring SIGHUP")});

  // Log if we crash.
  // process.on('uncaughtException', function (exception) {
  //   console.trace(exception);
  //   exitHandler();
  // });
  // process.on('unhandledRejection', (reason, p) => {
  //   console.log("Unhandled Rejection at: Promise ", p, " reason: ", reason);
  //   exitHandler();
  // });

  // Set up server.
  var configFile = process.argv[2];
  var port = parseInt(process.argv[3]);
  console.log("Using config: " + configFile);
  console.log("Using port: " + port);
  var config = yaml.load(fs.readFileSync(configFile, "utf8"));
  var server = createServer(config)
  server.listen(port);

  // Set up ZooKeeper.
//  gbifServiceRegistry.register(config);

  // Aim to exit cleanly.
//  process.on('SIGINT', exitHandler.bind());
//  process.on('SIGTERM', exitHandler.bind());
//  process.on('exit', exitHandler.bind());
//} catch (e) {
//  console.error(e);
//}
