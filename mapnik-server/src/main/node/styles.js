const fs = require('fs')
    , parser = require('./cartoParser');

/**
 * Compile the CartoCss into Mapnik stylesheets into a lookup dictionary
 */
var namedStyles = {};

// Heat styles (used in gbif.org)
namedStyles["purpleHeat.point"] = compileStylesheetSync("./cartocss/purple-heat-dot.mss")
namedStyles["blueHeat.point"] = compileStylesheetSync("./cartocss/blue-heat-dot.mss")
namedStyles["orangeHeat.point"] = compileStylesheetSync("./cartocss/orange-heat-dot.mss")
namedStyles["greenHeat.point"] = compileStylesheetSync("./cartocss/green-heat-dot.mss")

// Classic GBIF styling (used in gbif.org)
namedStyles["classic.point"] = compileStylesheetSync("./cartocss/classic-dot.mss")
namedStyles["classic.poly"] = compileStylesheetSync("./cartocss/classic-poly.mss")
// Also V1
namedStyles["classic-noborder.poly"] = compileStylesheetSync("./cartocss/classic-noborder-poly.mss")

// Purple yellow colour ramp (used in gbif.org)
namedStyles["purpleYellow.point"] = compileStylesheetSync("./cartocss/purple-yellow-dot.mss")
namedStyles["purpleYellow.poly"] = compileStylesheetSync("./cartocss/purple-yellow-poly.mss")
namedStyles["purpleYellow-noborder.poly"] = compileStylesheetSync("./cartocss/purple-yellow-noborder-poly.mss")

// Cluster styles (used in gbif.org)
namedStyles["outline.poly"] = compileStylesheetSync("./cartocss/outline-poly.mss")
namedStyles["blue.marker"] = compileStylesheetSync("./cartocss/blue-marker.mss")
namedStyles["orange.marker"] = compileStylesheetSync("./cartocss/orange-marker.mss")

// Miscellaneous styles
namedStyles["green.poly"] = compileStylesheetSync("./cartocss/green-poly.mss")
namedStyles["green2.poly"] = compileStylesheetSync("./cartocss/green2-poly.mss")
namedStyles["fire.point"] = compileStylesheetSync("./cartocss/fire-dot.mss")
namedStyles["glacier.point"] = compileStylesheetSync("./cartocss/glacier-dot.mss")

// Styles for compatibility with V1 API
namedStyles["iNaturalist.poly"] = compileStylesheetSync("./cartocss/iNaturalist-poly.mss")
namedStyles["purpleWhite.poly"] = compileStylesheetSync("./cartocss/purple-white-poly.mss")
namedStyles["red.poly"] = compileStylesheetSync("./cartocss/red-poly.mss")

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
var defaultStyle = "classic.point";

function Styles() {}

Styles.prototype.getStyleName = function(style) {
  return (style in namedStyles) ? style : defaultStyle;
}

Styles.prototype.getStyle = function(style) {
  return (style in namedStyles) ? namedStyles[style] : namedStyles[defaultStyle];
}

module.exports = new Styles();
