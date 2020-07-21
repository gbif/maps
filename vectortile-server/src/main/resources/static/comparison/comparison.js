proj4.defs('EPSG:4326', "+proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees");

var densityColours = ["#fed976ff", "#fd8d3ccd", "#fd8d3cb0", "#f03b2098", "#bd002698"];
var denLineColours = ["#fe9724ff", "#fd5b24cd", "#fd471db0", "#f0112998", "#bd004798"];

var pointStyles = [
  new ol.style.Style({
    image: new ol.style.Circle({
      fill: new ol.style.Fill({color: densityColours[0]}),
      stroke: new ol.style.Stroke({color: denLineColours[0]}),
      radius: 2
    }),
    fill: new ol.style.Fill({color: densityColours[0]})
  }),
  new ol.style.Style({
    image: new ol.style.Circle({
      fill: new ol.style.Fill({color: densityColours[1]}),
      radius: 3.5
    }),
    fill: new ol.style.Fill({color: densityColours[1]})
  }),
  new ol.style.Style({
    image: new ol.style.Circle({
      fill: new ol.style.Fill({color: densityColours[2]}),
      radius: 5
    }),
    fill: new ol.style.Fill({color: densityColours[2]})
  }),
  new ol.style.Style({
    image: new ol.style.Circle({
      fill: new ol.style.Fill({color: densityColours[3]}),
      radius: 8
    }),
    fill: new ol.style.Fill({color: densityColours[3]})
  }),
  new ol.style.Style({
    image: new ol.style.Circle({
      fill: new ol.style.Fill({color: densityColours[4]}),
      radius: 15
    }),
    fill: new ol.style.Fill({color: densityColours[4]})
  }),
];

function createDensityStyle() {
  var styles = [];
  return function(feature, resolution) {
    var length = 0;
    var magnitude = Math.trunc(Math.min(4, Math.floor(Math.log10(feature.get('total')))));
    styles[length++] = pointStyles[magnitude];
    styles.length = length;
    return styles;
  };
}

var pixel_ratio = parseInt(window.devicePixelRatio) || 1;

var tile_size = 512;
var max_zoom = 16;

var extent = 180.0;
var resolutions = Array(max_zoom+1).fill().map((_, i) => ( extent / tile_size / Math.pow(2, i) ));
var tile_grid_4326 = new ol.tilegrid.TileGrid({
  extent: ol.proj.get('EPSG:4326').getExtent(),
  minZoom: 0,
  maxZoom: 16,
  resolutions: resolutions,
  tileSize: tile_size,
});

var tile_grid_3857 = new ol.tilegrid.createXYZ({
  minZoom: 0,
  maxZoom: 16,
  tileSize: tile_size,
});

// Base layers
var base_raster_style = 'gbif-light';

var base4326 = new ol.layer.Tile({
  source: new ol.source.TileImage({
    projection: 'EPSG:4326',
    tileGrid: tile_grid_4326,
    tilePixelRatio: pixel_ratio,
    url: 'https://tile.gbif.org/4326/omt/{z}/{x}/{y}@'+pixel_ratio+'x.png?style='+base_raster_style,
    wrapX: true
  })
});

var base3857 = new ol.layer.Tile({
  source: new ol.source.TileImage({
    tileGrid: tile_grid_3857,
    tilePixelRatio: pixel_ratio,
    url: 'https://tile.gbif.org/3857/omt/{z}/{x}/{y}@'+pixel_ratio+'x.png?style='+base_raster_style,
    wrapX: true
  })
});


// 4326 (top) left
var left4326 = new ol.layer.VectorTile({
  renderMode: 'image',
  source: new ol.source.VectorTile({
    projection: 'EPSG:4326',
    format: new ol.format.MVT(),
    url: '../../occurrence/adhoc/{z}/{x}/{y}.mvt?srs=EPSG:4326&mode=GEO_CENTROID',
    tileGrid: tile_grid_4326,
    tilePixelRatio: 8
  }),
  style: createDensityStyle(),
  visible: true,
});

// 4326 (top) right
var right4326 = new ol.layer.VectorTile({
  renderMode: 'image',
  source: new ol.source.VectorTile({
    projection: 'EPSG:4326',
    format: new ol.format.MVT(),
    url: '../../occurrence/density/{z}/{x}/{y}.mvt?srs=EPSG:4326&bin=square&squareSize=64',
    tileGrid: tile_grid_4326,
    tilePixelRatio: 8
  }),
  style: createDensityStyle(),
  visible: true,
});

// 3875 (bottom) left
var left3857 = new ol.layer.VectorTile({
  renderMode: 'image',
  source: new ol.source.VectorTile({
    format: new ol.format.MVT(),
    url: '../../occurrence/adhoc/{z}/{x}/{y}.mvt?srs=EPSG:3857&mode=GEO_CENTROID',
    tileGrid: tile_grid_3857,
    tilePixelRatio: 8
  }),
  style: createDensityStyle(),
  visible: true,
});

// 3857 (bottom) right
var right3857 = new ol.layer.VectorTile({
  renderMode: 'image',
  source: new ol.source.VectorTile({
    format: new ol.format.MVT(),
    url: '../../occurrence/density/{z}/{x}/{y}.mvt?srs=EPSG:3857&bin=square&squareSize=64',
    tileGrid: tile_grid_3857,
    tilePixelRatio: 8
  }),
  style: createDensityStyle(),
  visible: true,
});

// View setup
var view4326 = new ol.View({
  center: [0, 0],
  projection: 'EPSG:4326',
  zoom: 4
});

var view3857 = new ol.View({
  center: [0, 0],
  projection: 'EPSG:3857',
  zoom: 4
});

// Map definitions
var map1 = new ol.Map({
  layers: [base4326, left4326],
  target: 'map1',
  view: view4326
});

var map2 = new ol.Map({
  layers: [base4326, right4326],
  target: 'map2',
  view: view4326
});

var map3 = new ol.Map({
  layers: [base3857, left3857],
  target: 'map3',
  view: view3857
});

var map4 = new ol.Map({
  layers: [base3857, right3857],
  target: 'map4',
  view: view3857
});

// Permalink URL anchors, 99% based on https://openlayers.org/en/latest/examples/permalink.html

// default zoom, center and rotation
var zoom4326 = 3;
var center4326 = [0, 0];
var rotation4326 = 0;
var zoom3857 = 3;
var center3857 = [0, 0];
var rotation3857 = 0;

if (window.location.hash !== '') {
  // try to restore center, zoom-level and rotation from the URL
  var hash = window.location.hash.split('&');
  if (hash.length === 2) {
    var hash4326 = hash[0].replace('#map4326=', '');
    var parts4326 = hash4326.split('/');
    if (parts4326.length === 4) {
      zoom4326 = parseInt(parts4326[0], 10);
      center4326 = [
        parseFloat(parts4326[1]),
        parseFloat(parts4326[2])
      ];
      rotation4326 = parseFloat(parts4326[3]);
    }

    var hash3857 = hash[1].replace('map3857=', '');
    var parts3857 = hash3857.split('/');
    if (parts3857.length === 4) {
      zoom3857 = parseInt(parts3857[0], 10);
      center3857 = [
        parseFloat(parts3857[1]),
        parseFloat(parts3857[2])
      ];
      rotation3857 = parseFloat(parts3857[3]);
    }
  }
}

view4326.setCenter(center4326);
view4326.setZoom(zoom4326);
view4326.setRotation(rotation4326);
view3857.setCenter(center3857);
view3857.setZoom(zoom3857);
view3857.setRotation(rotation3857);

var shouldUpdate = true;
var updatePermalink = function() {
  if (!shouldUpdate) {
    // do not update the URL when the view was changed in the 'popstate' handler
    shouldUpdate = true;
    return;
  }

  var center4326 = view4326.getCenter();
  var hash4326 = 'map4326=' +
      view4326.getZoom() + '/' +
      Math.round(center4326[0] * 100) / 100 + '/' +
      Math.round(center4326[1] * 100) / 100 + '/' +
      view4326.getRotation();
  var center3857 = view3857.getCenter();
  var hash3857 = 'map3857=' +
      view3857.getZoom() + '/' +
      Math.round(center3857[0] * 100) / 100 + '/' +
      Math.round(center3857[1] * 100) / 100 + '/' +
      view3857.getRotation();
  var state = {
    zoom4326: view4326.getZoom(),
    center4326: view4326.getCenter(),
    rotation4326: view4326.getRotation(),
    zoom3857: view3857.getZoom(),
    center3857: view3857.getCenter(),
    rotation3857: view3857.getRotation()
  };
  window.history.pushState(state, 'map', '#'+hash4326+'&'+hash3857);
};

map1.on('moveend', updatePermalink);
map3.on('moveend', updatePermalink);

// restore the view state when navigating through the history, see
// https://developer.mozilla.org/en-US/docs/Web/API/WindowEventHandlers/onpopstate
window.addEventListener('popstate', function(event) {
  if (event.state === null) {
    return;
  }
  view4326.setCenter(event.state.center4326);
  view4326.setZoom(event.state.zoom4326);
  view4326.setRotation(event.state.rotation4326);
  view3857.setCenter(event.state.center3857);
  view3857.setZoom(event.state.zoom3857);
  view3857.setRotation(event.state.rotation3857);
  shouldUpdate = false;
});
