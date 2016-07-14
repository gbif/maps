var fs = require('fs');
var path = require('path');
var mbgl = require('mapbox-gl-native');
var sharp = require('sharp');
var request = require('request');

var map = new mbgl.Map({
  request: function(req, callback) {
    request({
      url: req.url,
      encoding: null,
      gzip: true
    }, function (err, res, body) {
      if (err) {
        callback(err);
      } else if (res.statusCode == 200) {
        var response = {};

        if (res.headers.modified) { response.modified = new Date(res.headers.modified); }
        if (res.headers.expires) { response.expires = new Date(res.headers.expires); }
        if (res.headers.etag) { response.etag = res.headers.etag; }

        response.data = body;

        callback(null, response);
      } else {
        callback(new Error(JSON.parse(body).message));
      }
    });
  }
});

map.load(require('./style.json'));

console.time("maprender");

map.render({zoom: 2, center: [-90,45]}, function(err, buffer) {
  if (err) throw err;
  map.release();

  console.timeEnd("maprender")

  var image = sharp(buffer, {
    raw: {
      width: 512,
      height: 512,
      channels: 4
    }
  });

  // Convert raw image buffer to PNG
  image.toFile('image.png', function(err) {
    if (err) throw err;
  });
});




