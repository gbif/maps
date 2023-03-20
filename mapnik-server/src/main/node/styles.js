"use strict";

const fs = require('fs')
    , parser = require('./cartoParser');

/**
 * Compile the CartoCSS into Mapnik stylesheets into a lookup dictionary
 */
var namedStyles = {};
var heatStyles = [];
var hardPointStyles = [];

// Heat styles (used in gbif.org)
namedStyles["purpleHeat.point"] = compileStylesheetSync("./cartocss/purple-heat-dot.mss");
namedStyles["blueHeat.point"] = compileStylesheetSync("./cartocss/blue-heat-dot.mss");
namedStyles["orangeHeat.point"] = compileStylesheetSync("./cartocss/orange-heat-dot.mss");
namedStyles["greenHeat.point"] = compileStylesheetSync("./cartocss/green-heat-dot.mss");
heatStyles.push("purpleHeat.point", "blueHeat.point","orangeHeat.point", "greenHeat.point");

// Classic GBIF styling (used in gbif.org)
namedStyles["classic.point"] = compileStylesheetSync("./cartocss/classic-dot.mss");
hardPointStyles.push("classic.point");
namedStyles["classic.poly"] = compileStylesheetSync("./cartocss/classic-poly.mss");
// Also V1
namedStyles["classic-noborder.poly"] = compileStylesheetSync("./cartocss/classic-noborder-poly.mss");

// Purple yellow colour ramp (used in gbif.org)
namedStyles["purpleYellow.point"] = compileStylesheetSync("./cartocss/purple-yellow-dot.mss");
hardPointStyles.push("purpleYellow.point");
namedStyles["purpleYellow.poly"] = compileStylesheetSync("./cartocss/purple-yellow-poly.mss");
namedStyles["purpleYellow-noborder.poly"] = compileStylesheetSync("./cartocss/purple-yellow-noborder-poly.mss");

// Green colour ramp
namedStyles["green.point"] = compileStylesheetSync("./cartocss/green-dot.mss");
hardPointStyles.push("green.point");
namedStyles["green.poly"] = compileStylesheetSync("./cartocss/green-poly.mss");
namedStyles["green-noborder.poly"] = compileStylesheetSync("./cartocss/green-noborder-poly.mss");

// Cluster styles (used in gbif.org)
namedStyles["outline.poly"] = compileStylesheetSync("./cartocss/outline-poly.mss");
namedStyles["blue.marker"] = compileStylesheetSync("./cartocss/blue-marker.mss");
namedStyles["orange.marker"] = compileStylesheetSync("./cartocss/orange-marker.mss");

// Adhoc map style for ES portal (mode=GEO_CENTROID)
namedStyles["scaled.circles"] = compileStylesheetSync("./cartocss/scaled-circles.mss");

// Miscellaneous styles
namedStyles["fire.point"] = compileStylesheetSync("./cartocss/fire-dot.mss");
namedStyles["glacier.point"] = compileStylesheetSync("./cartocss/glacier-dot.mss");
heatStyles.push("fire.point", "glacier.point");

// Styles for compatibility with V1 API
namedStyles["green2.poly"] = compileStylesheetSync("./cartocss/green2-poly.mss");
namedStyles["green2-noborder.poly"] = compileStylesheetSync("./cartocss/green2-noborder-poly.mss");
namedStyles["iNaturalist.poly"] = compileStylesheetSync("./cartocss/iNaturalist-poly.mss");
namedStyles["purpleWhite.poly"] = compileStylesheetSync("./cartocss/purple-white-poly.mss");
namedStyles["red.poly"] = compileStylesheetSync("./cartocss/red-poly.mss");

var defaultStyle = "classic.point";

function compileStylesheetSync(filename) {
  // snippet simulating a TileJSON response from Tilelive, required only to give the layers for the CartoParser
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

function Styles() {}

Styles.prototype.getStyleName = function(style) {
  return (style in namedStyles) ? style : defaultStyle;
}

Styles.prototype.getStyle = function(style) {
  return (style in namedStyles) ? namedStyles[style] : namedStyles[defaultStyle];
}

/*
 * Hard pixel styles, where pixels should not be semitransparent.
 */
Styles.prototype.isHardPixelStyle = function(style) {
  return hardPointStyles.indexOf(style) > -1;
}

/*
 * Styles relying on marker overlap, which must be generated with 4 tiles one zoom level lower.
 */
Styles.prototype.isHeatStyle = function(style) {
  return heatStyles.indexOf(style) > -1;
}

module.exports = new Styles();
