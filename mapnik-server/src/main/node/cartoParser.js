"use strict";

var mapnik = require('mapnik')
  , tm = require('./tm')
  , _ = require('underscore')
  , carto = require('carto');

var parser = {}

/**
 * Method originally taken from mapbox and then stripped of much logic to reduce to a simple parser.
 * https://github.com/mapbox/mapbox-studio-classic/blob/mb-pages/lib/style.js#L192
 *
 * Requires:
 * - styles: An array of Strings each containing the CartoCSS
 * - tilejson: A String of tilejson metadata
 *
 * Returns mapnik stylesheet XML.
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
