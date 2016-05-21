var mapnik = require('mapnik')
  , http = require('http')
  , url = require('url')
  , fs = require('fs')
  , tm = require('./tm')
  , _ = require('underscore')
  , tilelive = require('tilelive')
  , carto = require('carto')
  , Vector = require('tilelive-vector');

// configure tilelive to a) use Mapnik and b) tilejson to find the vector tiles through config
Vector.registerProtocols(tilelive);
require('tilejson').registerProtocols(tilelive);

var data = {
  source: 'tilejson+http://localhost:3000/metadata4.json',
  styles: [fs.readFileSync('style.mss','utf8')]
}


toXML(data, function(err, xml) {
  if (err) return console.log(err);
  console.log("FINISHED1");
  console.log(xml);
})

console.log("FINISHED");


/**
 * Method originally taken from
 *   https://github.com/mapbox/mapbox-studio-classic/blob/mb-pages/lib/style.js#L192
 */
function toXML(data, callback) {
  tilelive.load(data.source, function(err, backend) {
    if (err) return callback(err);
    console.log("backend: " + backend);
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
      'template',
      'interactivity_layer',
      'legend'
    ].reduce(function(memo, key) {
      if (key in data) switch(key) {
        // @TODO this is backwards because carto currently only allows the
        // TM1 abstrated representation of these params. Add support in
        // carto for "literal" definition of these fields.
        case 'interactivity_layer':
          if (!backend.data) break;
          if (!backend.data.vector_layers) break;
          var fields = data.template.match(/{{([a-z0-9\-_]+)}}/ig);
          if (!fields) break;
          memo['interactivity'] = {
            layer: data[key],
            fields: fields.map(function(t) { return t.replace(/[{}]+/g,''); })
          };
          break;
        default:
          memo[key] = data[key];
          break;
      }
      return memo;
    }, {});

    // Set projection for Mapnik.
    opts.srs = tm.srs['900913'];

    // Convert datatiles sources to mml layers.
    var layers;
    if (data.layers) {
      console.log("datalayers: " + data.layers);

      layers = data.layers.map(function(l) {
        return { id: l.split('.')[0], 'class': l.split('.')[1] }
      });
      // Normal vector source (both remote + local)
    } else if (backend.data.vector_layers) {
      layers = backend.data.vector_layers;
      // Assume image source
    } else {
      layers = [{id:'_image'}];
    }

    if (opts.interactivity && backend.data.vector_layers) {
      var err = style.interactivityvalidate(backend.data.vector_layers, opts.interactivity);
      if (err) return callback(err);
    }

    opts.Layer = layers.map(function(layer) { return {
      id:layer.id,
      name:layer.id,
      'class':layer['class'],
      // Styles can provide a hidden _properties key with
      // layer-specific property overrides. Current workaround to layer
      // properties that could (?) eventually be controlled via carto.
      properties: (data._properties && data._properties[layer.id]) || {},
      srs:tm.srs['900913']
    } });

    opts.Stylesheet = _(data.styles).map(function(style,basename) { return {
      id: basename,
      data: style
    };

    });



    try {
      console.log("rendering: ");
      console.log("STYLE: " + JSON.stringify(opts.Stylesheet))
      var xml = new carto.Renderer(null, {
        mapnik_version: mapnik.versions.mapnik
      }).render(opts);

        //.render(tm.sortkeys(opts));

      console.log("rendered: " + xml);



    } catch(err) {
      console.log("error: " + xml);
      return callback(err);
    }
    return callback(null, xml);
  });
};
