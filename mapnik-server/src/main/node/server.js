"use strict";

const async = require('async')
    , fs = require('fs')
    , http = require('http')
    , request = require('request')
    , stoppable = require('stoppable')
    , config = require('./config')
    , gbifServiceRegistry = require('./gbifServiceRegistry')
    , renderer = require('./renderer')
    , routes = require('./routes')
    , styles = require('./styles');

var server;

function createServer(config) {
  var timeout = config.tileServer.timeout;

  return http.createServer(function(req, res) {
    console.log("Request:", req.url);

    try {
      var x = routes(req, res);
      if (x) {
        var parameters = x.parameters;
        var vectorTileUrl = x.vectorTileUrl;
        var heatVectorTileUrls = x.heatVectorTileUrls;
      } else {
        res.writeHead(400, {'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'X-Error': 'Bad request?'});
        res.end(fs.readFileSync('./public/map/400.png'));
        return;
      }

      // For heat maps, retrieve the four tiles one zoom level in, and join together.
      if (styles.isHeatStyle(parameters.style)) {
        async.map(heatVectorTileUrls,
          function (url, callback) {
            console.time("Heat get "+url);
            request.get({url: url, method: 'GET', encoding: null, gzip: true, timeout: timeout}, function (error, response, body) {
              console.timeEnd("Heat get "+url);
              if (!error && response.statusCode >= 300) {
                error = {
                  'code': response.statusCode,
                  'message': 'Error from backend (vector tile); try '+url+' yourself'
                };
              }
              console.log(url, "Vector tile has HTTP status", response ? response.statusCode : '-', "and size", body ? body.length : '-');
              callback(error, {body: body, etag: response ? response.headers['etag'] : null});
            })
          },
          function (err, results) {
            console.log("Retrieved", results.length, "tiles, error", err);
            if (!err && results.length == 4) {
              writeHeaders(200, results[0].etag, res);
              renderer(parameters, results.map(r => r.body), res);
            } else {
              console.log("Error retrieving four vector tiles", err);
              res.writeHead(503, {'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'X-Error': err.message});
              res.end(fs.readFileSync('./public/map/503.png'));
            }
          });
        return;
      }

      // issue the request to the vector tile server and render the tile as a PNG using Mapnik
      console.time("Get "+vectorTileUrl);
      request.get({url: vectorTileUrl, method: 'GET', encoding: null, gzip: true, timeout: timeout}, function (error, response, body) {
        console.timeEnd("Get "+vectorTileUrl);

        if (!error && response.statusCode >= 300) {
          error = {
            'code': response.statusCode,
            'message': 'Error from backend (vector tile); try '+vectorTileUrl+' yourself'
          };
        }

        if (error) {
          // something went wrong
          console.log("Error retrieving vector tile", error);
          res.writeHead(503, {'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'X-Error': error.message});
          res.end(fs.readFileSync('./public/map/503.png'));
          return;
        }

        console.log("Vector tile has HTTP status", response.statusCode, "and size", body.length);

        if (response.statusCode == 200 && body.length > 0) {
          writeHeaders(200, response.headers['etag'], res);

          try {
            renderer(parameters, body, res);
          } catch (e) {
            // something went wrong
            res.writeHead(500, {'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'X-Error': e.message});
            res.end(fs.readFileSync('./public/map/500.png'));
            console.log(e);
          }

        } else if (response.statusCode == 404 ||   // not found
                   response.statusCode == 204 ||   // no content
                   (response.statusCode == 200 && body.length == 0)) {  // accepted but no content
          // no tile
          writeHeaders((response.statusCode == 200) ? 204 : response.statusCode, // keep same status code, except empty 200s.
            response.headers['etag'], res);
          res.end();
        } else {
          res.writeHead(500, {'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'X-Error': 'This happening would be quite strange, e.g. other 400 responses'});
          res.end(fs.readFileSync('./public/map/500.png'));
        }
      })
    } catch (e) {
      console.log("Unexpected error:");
      console.error(e);
      try {
        res.writeHead(500, {'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'X-Error': 'Unexpected exception'});
        res.end(fs.readFileSync('./public/map/500.png'));
      } catch(e2) {
        console.log("Failed to write error response");
        console.error(e2);
      }
    }
  });
}

function writeHeaders(status, etagIn, res) {
  // Pass along an ETag if present, to aid caching.
  // (NB reliant on Varnish to handle If-None-Match request, we always return 200 or 204.)
  var etagOut;
  if (etagIn) {
    etagOut = etagIn;
  } else {
    etagOut = '"' + Date.now() + '"';
  }

  res.writeHead(status, {
    'Content-Type': 'image/png',
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'public, max-age=600',
    'ETag': etagOut,
  });
}

/**
 * Shut down cleanly.
 */
function exitHandler() {
  console.log("Completing requests");
  server.stop();
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
  var port = parseInt(process.argv[3]);
  console.log("Using port: " + port);
  server = stoppable(createServer(config), 1000);
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
