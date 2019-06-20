"use strict";

const async = require('async')
    , mapnik = require('mapnik')
    , styles = require('./styles');

const proj4 = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over';

function renderTile(parameters, vectorTile, callback) {
  var isHardPixelStyle = styles.isHardPixelStyle(parameters.style);
  var isHeatStyle = styles.isHeatStyle(parameters.style);
  var scale = isHardPixelStyle ? 1 : parameters.density;

  // Hard pixel styles are produced at double vector tile resolution, then resized.
  // (NB point vector tiles have extent 512, but binned vector tiles have extent 4096.)
  var size = isHardPixelStyle ? 1024 : 512 * parameters.density;

  if (!vectorTile || vectorTile.length == 0) {
    console.log("Empty tile");
    var image = new mapnik.Image(size, size, {premultiplied: true});
    callback(null, image);
    return;
  }

  var map = new mapnik.Map(size, size, proj4);
  map.fromStringSync(styles.getStyle(parameters.style));

  // Pretend it's tile 0, 0, since Mapnik validates the address according to the standard Google schema,
  // and we aren't using it for WGS84.
  var vt = new mapnik.VectorTile(parameters.z, 0, 0);
  vt.addDataSync(vectorTile);

  //console.time("Render");
  vt.render(map, new mapnik.Image(size, size), {
    "buffer_size": 32, // important to include a buffer, to catch the overlaps
    "scale": scale
  }, function (err, image) {
    //console.timeEnd("Render");

    if (err) {
      console.log("ERROR", err);
      callback(err, null);
    }
    else {
      // For hard pixel styles, produce them at double resolution, then reduce to the correct size and set transparency
      // to all-or-nothing.
      if (isHardPixelStyle) {
        image = manipulatePixelStyle(image, parameters.density);
      }

      if (isHeatStyle) {
        image.premultiply();
      }
      callback(err, image);
    }
  });
}

function render(parameters, vectorTile, res) {
  async.map([vectorTile],
    function(tile, callback) {renderTile(parameters, tile, callback)},
    function(err, results) {
      if (err) console.log('One rendered', results, err);

      res.end(results[0].encodeSync('png'));
    });
}

/*
 * Render a heat map style, from four vector tiles.
 */
function heatMapRender(parameters, vectorTiles, res) {
  // Hard pixel styles are produced at double resolution, then resized.
  var size = 512 * parameters.density;

  async.map(vectorTiles,
    function(tile, callback) {renderTile(parameters, tile, callback)},
    function(err, results) {
      if (err) console.log('All rendered', results, err);

      var image = new mapnik.Image(size * 2, size * 2, {premultiplied: true});

      image.composite(results[0], {comp_op: mapnik.compositeOp['multiply'], dx: 0, dy: 0, opacity: 1.0},
        function(err, result) {
          image.composite(results[1], {comp_op: mapnik.compositeOp['multiply'], dx: size, dy: 0, opacity: 1.0},
            function(err, result) {
              image.composite(results[2], {comp_op: mapnik.compositeOp['multiply'], dx: 0, dy: size, opacity: 1.0},
                function(err, result) {
                  image.composite(results[3], {comp_op: mapnik.compositeOp['multiply'], dx: size, dy: size, opacity: 1.0},
                    function(err, result) {

                      result.premultiply();
                      image = result.resizeSync(size, size);
                      res.end(image.encodeSync('png'));
                    });
                });
            });
        });
    });
}

/* Mapnik renders pixels aligned exactly to the tile grid (i.e. centred *between* the pixels).
   That leads to every pixel being semi-transparent due to anti-aliasing.
   We render 2px wide pixels on a double-size tile, then throw away alternate rows and columns
   to produce the normal size (512).  Coloured pixels are set to opaque.
 */
function manipulatePixelStyle(image, density) {
  var size = image.width() / 2;

  //console.time("resize");
  // The method available in node-mapnik to resize don't work correctly — a pixel around the edge is lost,
  // and the transparency calculation makes things blurry.

  // This converts the 1024px image to a 512px image, and throws away semitransparency.
  var doubleSize = image.data();
  // A 3-wide 2-high image would look like:
  // RGBA-RGBA-RGBA
  // RGBA-RGBA-RGBA
  var correctSize = new Buffer(doubleSize.length / 4);
  for (var j = 0; j < size; j += 1) {
    for (var i = 0; i < 4 * size; i += 4) {
      correctSize[4 * size * j + i + 0] = doubleSize[4 * 4 * size * j + 2 * i + 0];
      correctSize[4 * size * j + i + 1] = doubleSize[4 * 4 * size * j + 2 * i + 1];
      correctSize[4 * size * j + i + 2] = doubleSize[4 * 4 * size * j + 2 * i + 2];
      if (correctSize[4 * size * j + i] + correctSize[4 * size * j + i + 1] + correctSize[4 * size * j + i + 2] > 0) {
        correctSize[4 * size * j + i + 3] = 255;
      }
      else {
        correctSize[4 * size * j + i + 3] = 0;
      }
    }
  }

  var image = new mapnik.Image.fromBufferSync(size, size, correctSize);

  // Scale the image for hi-DPI tiles if required.
  if (density != 1) {
    image.premultiply();
    image = image.resizeSync(size * density, size * density);
  }

  //console.timeEnd("resize");
  return image;
}

module.exports = function(parameters, vectorTile, res) {
  if (Array.isArray(vectorTile)) {
    heatMapRender(parameters, vectorTile, res);
  } else {
    render(parameters, vectorTile, res);
  }
}
