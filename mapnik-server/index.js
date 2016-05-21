var fs = require('fs');
var tilelive = require('tilelive');
var Vector = require('tilelive-vector');
Vector.registerProtocols(tilelive);
require('tilelive-mapnik').registerProtocols(tilelive);
require('tilelive-file').registerProtocols(tilelive);
require('tilejson').registerProtocols(tilelive);
mercator = require('./sphericalmercator');
var mapnik = require('mapnik');
//var request = require('requestretry');
//request.setDefaults({timeout: 1});



tilelive.load('tilejson+file:///Users/tim/dev/git/gbif/maps/mapnik-server/metadata.json', function(err, source) {

    if (err) throw err;


    // Interface is in XYZ/Google coordinates.
    // Use `y = (1 << z) - 1 - y` to flip TMS coordinates.
    //source.getTile(0, 0, 0, function(err, tile, headers) {
    //source.getTile(15, 16651, 10934, function(err, tile, headers) {
    source.getTile(13,4162,2733, function(err, tile, headers) {
        if (err) throw err;

        var map = new mapnik.Map(512, 512);
        map.loadSync('./stylesheet.xml');

        // log info
        console.log(mapnik.VectorTile.info(tile));

        // http://localhost:7001/api/all/15/16651/10934.pbf
        //var vt = new mapnik.VectorTile(0,0,0);
        //var vt = new mapnik.VectorTile(15,16651,10934);
        var vt = new mapnik.VectorTile(13,4162,2733);
        vt.addDataSync(tile);

        vt.render(map, new mapnik.Image(512,512), function(err, image) {
            if (err) throw err;
            image.save('./tile.png', 'png32');
        });
    });
});
