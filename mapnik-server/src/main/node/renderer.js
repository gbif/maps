const mapnik = require('mapnik')
    , mercator = require('./sphericalmercator');

function render(parameters, vectorTile, res) {

  var body = vectorTile;

  var dotStyle = parameters.dotStyle;

  // Pixel styles are produced at double resolution, then resized.
  var size = (dotStyle) ? 1024 : 512 * parameters.density;

  console.time('map');
  var map = new mapnik.Map(size, size, mercator.proj4);
  map.fromStringSync(parameters.stylesheet);
  console.timeEnd('map');
  // Pretend it's tile 0, 0, since Mapnik validates the address according to the standard Google schema,
  // and we aren't using it for WGS84.
  var vt = new mapnik.VectorTile(parameters.z, 0, 0);
  vt.addDataSync(body);

  // important to include a buffer, to catch the overlaps
  console.time("render");
  vt.render(map, new mapnik.Image(size, size), {
    "buffer_size": 8,
    "scale": dotStyle ? 1 : parameters.density
  }, function (err, image) {
    console.timeEnd("render");

    if (err) {
      res.end(err.message);
      return;
    }

    // For dotStyles, produce them at double resolution, then reduce to the correct size and set transparency
    // to all-or-nothing.
    if (dotStyle) {
      image = manipulatePixelStyle(image, parameters.density);
    }

    image.encode('png', function (err, buffer) {
      if (err) {
        res.end(err.message);
        return;
      }
      console.log('success');
      res.end(buffer);
    });
  });
}

/* Mapnik renders pixels aligned exactly to the tile grid (i.e. centred *between* the pixels).
   That leads to every pixel being semi-transparent due to anti-aliasing.
   We render 2px wide pixels on a double-size tile, then throw away alternate rows and columns
   to produce the normal size (512).  Coloured pixels are set to opaque.
 */
function manipulatePixelStyle(image, density) {
  size = 512;

  console.time("resize");
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
    image = image.resizeSync(512 * density, 512 * density);
  }

  console.timeEnd("resize");
  return image;
}

module.exports = function(parameters, vectorTile, res) {
  render(parameters, vectorTile, res);
}
