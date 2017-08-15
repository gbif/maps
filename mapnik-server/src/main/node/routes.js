const request = require('request')
    , http = require('http')
    , url = require('url')
    , fs = require('fs')
    , styles = require('./styles');

/**
 * The server supports the ability to provide assets which need to be explicitly registered in order to be secure.
 * (e.g. trying to expose files using URL hack such as http://api.gbif.org/v1/map/../../../hosts)
 *
 * Should this become more complex, then express or similar should be consider.
 */
var assetsHTML = [
  '/map/styles.html',
  '/map/demo.html',
  '/map/demo1.html',
  '/map/demo2.html',
  '/map/demo3.html',
  '/map/demo4.html',
  '/map/demo5.html',
  '/map/demo6.html',
  '/map/demo7.html',
  '/map/demo8.html',
  '/map/demo9.html',
  '/map/demo10.html',
  '/map/demo11.html',
  '/map/demo12.html',
  '/map/demo-cartodb.html',
  '/map/binning-debugging.html',
  '/map/legacy-style-debugging.html',
  '/map/style-debugging.html'];


function parseUrl(parsedRequest) {
  if (parsedRequest.pathname.endsWith(".png")) {

    // extract the x,y,z from the URL which could be /some/map/type/{z}/{x}/{y}@{n}x.png?srs=EPSG:4326&...
    var dirs = parsedRequest.pathname.substring(0, parsedRequest.pathname.length - 7).split("/");
    var z = parseInt(dirs[dirs.length - 3]);
    var x = parseInt(dirs[dirs.length - 2]);
    var y = parseInt(dirs[dirs.length - 1]);

    // find the compiled stylesheet from the given style parameter, defaulting if omitted or bogus
    var style = parsedRequest.query.style; // TODO (parsedRequest.query.style in namedStyles) ? parsedRequest.query.style : defaultStyle;
    var stylesheet = styles(style);
    var dotStyle = style.indexOf('point') > -1 && style.indexOf('Heat') < 0;

    var sDensity = parsedRequest.pathname.substring(parsedRequest.pathname.length - 6, parsedRequest.pathname.length - 5);
    var density = (sDensity == 'H') ? 0.5 : parseInt(sDensity);

    if (!(isNaN(z) || isNaN(x) || isNaN(y) || isNaN(density))) {
      return {
        "z": z,
        "x": x,
        "y": y,
        "density": density,
        "stylesheet": stylesheet,
        "dotStyle": dotStyle,
      }
    }
  }
  throw Error("URL structure is invalid, expected /some/map/type/{z}/{x}/{y}@{n}x.png?srs=EPSG:4326&...");
}

/**
 * Parse /v1 GBIF API URLs, and handle as much as we can.
 *
 * This is not my nicest code.  Hopefully you don't need to touch it.
 *
 * We don't handle:
 * - "resolution" (1, 2, 4 or 8px dots on the map)
 * - colours, other than those used by our own map widget
 * - non-contiguous year ranges
 * - "no year" combined with a non-complete year range
 */
function v1ParseUrl(parsedRequest) {
  var z = parseInt(parsedRequest.query.z);
  var x = parseInt(parsedRequest.query.x);
  var y = parseInt(parsedRequest.query.y);

  // This gets us 256 pixel tiles.
  var density = 0.5;

  var resolution = parsedRequest.query.resolution;
  var squareSize;
  switch (resolution) {
    case  "2": squareSize =  32; break;
    case  "4": squareSize =  64; break;
    case  "8": squareSize = 128; break;
    case "16": squareSize = 256; break;
    default  : squareSize =  16;
  }

  var stylesheet = styles('classic-noborder.poly');
  if (parsedRequest.query.saturation == "true") {
    stylesheet = styles('purpleWhite.poly');
  } else if (parsedRequest.query.colors == ",,#CC0000FF") {
    stylesheet = styles('red.poly');
  } else if (parsedRequest.query.palette == "reds" || parsedRequest.query.colors == ',10,#F7005Ae6|10,100,#D50067e6|100,1000,#B5006Ce6|1000,10000,#94006Ae6|10000,100000,#72005Fe6|100000,,#52034Ee6') {
    stylesheet = styles('iNaturalist.poly');
  }

  var type = parsedRequest.query.type;
  var key = parsedRequest.query.key;

  var mapKey;
  if (type == "TAXON") mapKey = "taxonKey="+key
  else if (type == "DATASET") mapKey = "datasetKey="+key
  else if (type == "PUBLISHER") mapKey = "publishingOrg="+key
  else if (type == "COUNTRY") mapKey = "country="+key
  else if (type == "PUBLISHING_COUNTRY") mapKey = "publishingCountry="+key
  else mapKey = null;

  var basisOfRecord = new Set();

  // Year ranges.  We assume ranges given are continuous, but don't permit showing no-year records as well as a range —
  // unless the whole range (pre-1900 to 2020) is selected, then no year filter is requested.
  var
    obsStart = 9999,
    obsEnd = -1,
    spStart = 9999,
    spEnd = -1,
    othStart = 9999,
    othEnd = -1;
  var noYear = false;
  var obs, sp, oth;

  var layers = parsedRequest.query.layer;
  if (layers && !Array.isArray(layers)) {
    layers = [layers];
  } else if (!layers) {
    layers = [];
  }

  for (var l of layers) {
    if (l == "LIVING") {
      basisOfRecord.add("LIVING_SPECIMEN");
    } else if (l == "FOSSIL") {
      basisOfRecord.add("FOSSIL_SPECIMEN");
    } else {
       var first_ = l.indexOf('_');
       var second_ = l.indexOf('_', first_+1);

       var prefix = l.substring(0, first_);
       var startYear = l.substring(first_+1, second_);
       var endYear = l.substring(second_+1);

       if (startYear == "NO") {
         noYear = true;
       } else if (startYear == "PRE") {
         startYear = 0;
       }

       switch (prefix) {
         case "OBS":
           basisOfRecord.add("OBSERVATION");
           basisOfRecord.add("HUMAN_OBSERVATION");
           basisOfRecord.add("MACHINE_OBSERVATION");
           if (startYear != "NO") {
             obsStart = Math.min(obsStart, startYear);
             obsEnd = Math.max(obsEnd, endYear);
           }
           obs = true;
           break;
         case "SP":
           basisOfRecord.add("PRESERVED_SPECIMEN");
           if (startYear != "NO") {
             spStart = Math.min(spStart, startYear);
             spEnd = Math.max(spEnd, endYear);
           }
           sp = true;
           break;
         case "OTH":
           basisOfRecord.add("MATERIAL_SAMPLE");
           basisOfRecord.add("LITERATURE");
           basisOfRecord.add("UNKNOWN");
           if (startYear != "NO") {
             othStart = Math.min(othStart, startYear);
             othEnd = Math.max(othEnd, endYear);
           }
           oth = true;
           break;
         default:
       }
     }
  }

  // If all are selected, don't filter.
  if (basisOfRecord.size == 9) {
    basisOfRecord.clear();
  }

  var year;

  // All year filters must apply to all record types.
  var yearsMismatch = false;
  yearsMismatch &= obs && sp  && (obsStart !=  spStart || obsEnd !=  spEnd);
  yearsMismatch &= obs && oth && (obsStart != othStart || obsEnd != othEnd);
  yearsMismatch &=  sp && oth && ( spStart != othStart ||  spEnd != othEnd);

  if (!yearsMismatch) {
    if (obs && obsStart == 9999 || sp && spStart == 9999 || oth && othStart == 9999) {
      year = null;
    } else if (obs) {
      year = obsStart + "," + obsEnd;
    } else if (sp) {
      year = spStart + "," + spEnd;
    } else if (oth) {
      year = othStart + "," + othEnd;
    } else {
      // Only fossils and/or living
      year = null;
    }
  } else {
    var detail = "OBS "+obsStart+"-"+obsEnd+"; "+
        "SP "+spStart+"-"+spEnd+"; "+
        "OTH "+othStart+"-"+othEnd+"\n";

    throw Error("Start and end years must be the same for each layer (BasisOfRecord): "+detail);
  }

  if (year == "0,2020" && noYear) {
    year = null;
  } else if (year && noYear) {
    throw Error("Can't display undated records as well as a range of dated ones.\n");
  }

  return {
    "z": z,
    "x": x,
    "y": y,
    "density": density,
    "stylesheet": stylesheet,
    "year": year,
    "key": mapKey,
    "basisOfRecord": Array.from(basisOfRecord),
    "dotStyle": false,
    "squareSize": squareSize
  }
}

function vectorRequest(parsedRequest) {
  // reformat the request to the type expected by the VectorTile Server
  // Remove raster parameters, to improve cacheability
  delete parsedRequest.query.style;
  delete parsedRequest.search; // Must be removed to force regeneration of query string
  parsedRequest.pathname = parsedRequest.pathname.replace("@Hx.png", ".mvt");
  parsedRequest.pathname = parsedRequest.pathname.replace("@1x.png", ".mvt");
  parsedRequest.pathname = parsedRequest.pathname.replace("@2x.png", ".mvt");
  parsedRequest.pathname = parsedRequest.pathname.replace("@3x.png", ".mvt");
  parsedRequest.pathname = parsedRequest.pathname.replace("@4x.png", ".mvt");
  parsedRequest.hostname = 'api.gbif.org'; //config.tileServer.host;
  parsedRequest.port = 80; //config.tileServer.port;
  parsedRequest.pathname = '/v2' + parsedRequest.pathname; //config.tileServer.prefix + parsedRequest.pathname;
  parsedRequest.protocol = "http:";
  return url.format(parsedRequest);
}

function v1VectorRequest(z, x, y, year, key, basisOfRecord, squareSize, parsedRequest) {
  delete parsedRequest.search; // Must be removed to force regeneration of query string

  var params = "srs=EPSG:3857";

  if (year) {
    params += "&year=" + year;
  }

  if (basisOfRecord.length > 0) {
    params += "&basisOfRecord=" + basisOfRecord.join("&basisOfRecord=");
  }

  if (key) {
    params += "&" + key;
  }

  if (squareSize) {
    params += "&bin=square&squareSize=" + squareSize;
  }

  parsedRequest.search = params;

  parsedRequest.pathname = "/map/occurrence/density/"+z+"/"+x+"/"+y+".mvt";
  parsedRequest.hostname = config.tileServer.host;
  parsedRequest.port = config.tileServer.port;
  parsedRequest.pathname = config.tileServer.prefix + parsedRequest.pathname;
  parsedRequest.protocol = "http:";
  return url.format(parsedRequest);
}






module.exports = function(req, res) {

    var parsedRequest = url.parse(req.url, true)

    // handle registered assets
    if (assetsHTML.indexOf(parsedRequest.path) != -1) {
      res.writeHead(200, {'Content-Type': 'text/html'});
      res.end(fs.readFileSync('./public' + parsedRequest.path));
      return;
    } else {
      // Handle map tiles.

      var parameters, vectorTileUrl;
      if (parsedRequest.pathname.indexOf('tile') > 0) {
        try {
          console.log("Legacy try");
          parameters = v1ParseUrl(parsedRequest);
          vectorTileUrl = v1VectorRequest(parameters.z, parameters.x, parameters.y, parameters.year, parameters.key, parameters.basisOfRecord, parameters.squareSize, parsedRequest);
        } catch (e) {
          res.writeHead(410, {
            'Content-Type': 'image/png',
            'Access-Control-Allow-Origin': '*',
            'X-Error': "Some features of GBIF's V1 map API have been deprecated.  See http://www.gbif.org/developer/maps for further information.",
            'X-Error-Detail': e.message
          });
          res.end(fs.readFileSync('./public/map/deprecated-warning-tile.png'));
          console.log("V1 request failed", e.message);
          return;
        }
      } else {
        try {
          parameters = parseUrl(parsedRequest);
          vectorTileUrl = vectorRequest(parsedRequest);

        } catch (e) {
          res.writeHead(400, {
            'Content-Type': 'image/png',
            'Access-Control-Allow-Origin': '*',
            'X-Error': "Unable to parse request; see http://www.gbif.org/developer/maps for information.",
            'X-Error-Detail': e.message
          });
          res.end(fs.readFileSync('./public/map/400.png'));
          console.log("V2 request failed", e.message);
          return;
        }
      }
    }

  return {"parameters": parameters, "vectorTileUrl": vectorTileUrl};

}
