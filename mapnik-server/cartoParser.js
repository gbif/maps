var mapnik = require('mapnik')
  , tm = require('./tm')
  , _ = require('underscore')
  , tilelive = require('tilelive')
  , carto = require('carto');

// configure tilelive to a) use Mapnik and b) tilejson to find the vector tiles through config
require('tilejson').registerProtocols(tilelive);

var parser = {}

/**
 * Method originally taken from mapbox and then stripped of much logic to reduce to a simple parser.
 * https://github.com/mapbox/mapbox-studio-classic/blob/mb-pages/lib/style.js#L192
 *
 * Requires data to contain 2 fields:
 * - source: The http location of a tilejson metadata document, declared using tilejson+http://
 * - styles: An array of JSON Strings each containing CartoCSS
 *
 * An example might be:
 *
 * var data = {
 *  source: 'tilejson+http://localhost:7001/api/taxon/1/metadata.json',
 *  styles: [fs.readFileSync('gbif-classic.mss','utf8')]
 * }
 */

parser.parseToXML = function(styles, tilejson) {
  // Include params to be written to XML.
  var opts = [
    'name',
    'description',
    'attribution',
    'bounds',
    'center',
    'format',
    'minzoom',
    'maxzoom',
    'source',
    'template'
  ]
  opts.srs = tm.srs['900913'];

  // Convert datatiles sources to mml layers.
  var layers = tilejson.data.vector_layers;

  opts.Layer = layers.map(function(layer) {
    return {
      id:layer.id,
      name:layer.id,
      'class':layer['class'],
      srs:tm.srs['900913']
    }
  });

  opts.Stylesheet = _(styles).map(function(style,basename) {
    return {
      id: basename,
      data: style
    };
  });

  try {
    return new carto.Renderer(null, {
      mapnik_version: mapnik.versions.mapnik
    }).render(opts);

  } catch(err) {
    throw ("Error parsing CartoCSS: " + err.message);
  }
}

module.exports = parser;
