// GBIF Map base layer definitions, for OpenLayers.
const gbif_layers = new GBIFLayers();

var views = {
  "3857": new ol.View({
    center: ol.proj.fromLonLat([0, 0], 'EPSG:3857'),
    projection: ol.proj.get('EPSG:3857'),
    zoom: 2,
  }),
  "4326": new ol.View({
    center: [0, 0],
    projection: ol.proj.get('EPSG:4326'),
    zoom: 2,
  }),
  "3575": new ol.View({
    center: [0, 0],
    projection: ol.proj.get('EPSG:3575'),
    zoom: 2,
  }),
  "3031": new ol.View({
    center: [0, 0],
    extent: ol.proj.get('EPSG:3031').getExtent(),
    projection: ol.proj.get('EPSG:3031'),
    zoom: 2,
  }),
};

var map;
var initialized = false;
var srs;

var baseLayerGroup = new ol.layer.Group({
  layers: []
});

var gridLayerGroup = new ol.layer.Group({
  layers: []
});

var dataLayerGroup = new ol.layer.Group({
  layers: []
});

var data_layer = 'occurrence';
var source = 'density';
var render = 'vector';
var raster_style = 'classic-noborder.poly';
var basemap_style = 'gbif-classic';
var basemap_language = '';
var show_grid = false;
var binning = {};
var years = {};
var bors = {};
var map_key = {};

map = new ol.Map({
  layers: [baseLayerGroup, dataLayerGroup, gridLayerGroup],
  target: 'map',
  loadTilesWhileInteracting: true,
  loadTilesWhileAnimating: true
});

function setProjection() {
  var newSrs = document.querySelector('input[name=projection]:checked').value;

  console.log("Setting projection to "+newSrs);

  srs = newSrs;

  updateBaseLayer();
  updateDataLayer();
  updateGridLayer();
  map.setView(views[srs]);

  //currentLayers['grid'] = gbif_layers.grid(srs);
  //l.push(currentLayers['grid']);
  //bindInputs('grid');
}

function updateBaseLayer() {
  if (!initialized) {
    return;
  }

  console.log("Updating base layer");

  var l = gbif_layers.baseRaster(srs, {'style': basemap_style + basemap_language});
  base_url_template.value = l.getSource().getUrls()[0];

  baseLayerGroup.getLayers().clear();
  baseLayerGroup.getLayers().extend([l]);
  //console.log('New baselayer', l, base_url_template.value);
}

function updateDataLayer() {
  if (!initialized) {
    return;
  }

  console.log("Updating data layers");

  var search = {};
  Object.assign(search, binning);
  Object.assign(search, years);
  Object.assign(search, bors);
  Object.assign(search, map_key);
  Object.assign(search, mask_key);

  var raster_search = {};
  Object.assign(raster_search, search);
  Object.assign(raster_search, {'style': raster_style});

  var l;

  if (render == 'vector') {
    console.log("Search terms are", search);
    l = gbif_layers.occurrenceVector(srs, data_layer, source, search);
  } else {
    console.log("Search terms (raster) are", raster_search);
    l = gbif_layers.occurrenceRaster(srs, data_layer, source, raster_search);
  }

  data_url_template.value = l.getSource().getUrls()[0];

  dataLayerGroup.getLayers().clear();
  dataLayerGroup.getLayers().extend([l]);
  //console.log('layers', map.getLayers());
}

function updateGridLayer() {
  if (!initialized) {
    return;
  }

  console.log("Updating grid layer");

  gridLayerGroup.getLayers().clear();
  if (show_grid) {
    var l = gbif_layers.grid(srs, {});
    gridLayerGroup.getLayers().extend([l]);
  }
  //console.log('layers', map.getLayers());
}

function setDataLayer() {
  data_layer = document.querySelector('input[name=datalayer]:checked').value;

  console.log("Setting data layer to", data_layer);
  updateDataLayer();
}

function setSource() {
  source = document.querySelector('input[name=datasource]:checked').value;

  console.log("Setting source to", source);
  updateDataLayer();
}

function setRender() {
  render = document.querySelector('input[name=render]:checked').value;

  console.log("Setting render to", render);
  updateDataLayer();
}

function setRasterStyle() {
  raster_style = document.querySelector('input[name=raster_style]:checked').value;

  console.log("Setting raster style to", raster_style);
  updateDataLayer();
}

function setBasemapStyle() {
  basemap_style = document.querySelector('input[name=basemap_style]:checked').value;
  basemap_language = document.querySelector('#basemap_language').value;

  console.log("Setting basemap style to", basemap_style, basemap_language);
  updateBaseLayer();
}

function setGrid() {
  show_grid = document.getElementById('tile_grid').checked;

  console.log("Toggling grid to", show_grid);
  updateGridLayer();
}

function setBinning() {
  var binningOpt = document.querySelector('input[name=binning]:checked').value;

  binning = {};
  if (binningOpt == 'hex' || binningOpt == 'square') {
    var type = binningOpt == 'hex' ? 'hexPerTile' : 'squareSize';
    binning['bin'] = binningOpt;
    if (binningOpt == 'square') {
      binning[type] = Math.pow(2, document.getElementById('binning_'+binningOpt+'_size').value);
    } else {
      binning[type] = 201 - document.getElementById('binning_'+binningOpt+'_size').value;
    }
    density_style = {'style': 'classic-noborder.poly'};
  } else {
    density_style = {'style': 'classic.points'};
  }

  console.log("Setting binning to", binning);
  updateDataLayer();
}

function setYears() {
  var yearOpt = document.querySelector('input[name=year]:checked').value;

  years = {};
  if (yearOpt != 'all') {
    years['year'] = document.getElementById('year_range').value;
  }

  console.log("Setting years to", years);
  updateDataLayer();
}

function setBors() {
  bors = {};
  bors_a = [];

  var select_bor = document.getElementsByName('basis_of_record');
  for (var i = 0; i < select_bor.length; i++) {
    if (select_bor[i].checked) {
      bors_a.push(select_bor[i].value);
    }
  }

  if (bors_a.length == 0 || bors_a.length == select_bor.length) {
    bors_a = [];
  } else {
    var build = '';
    for (var i = 0; i < bors_a.length; i++) {
      build = build + bors_a[i] + '&basisOfRecord=';
    }
    build = build.slice(0, -'&basisOfRecord='.length);
    bors['basisOfRecord'] = build;
  }

  console.log("Setting bors to", bors);
  updateDataLayer();
}

function setMapKey(input) {
  if (input != null) {
    map_key_input.value = input;
  }

  map_key = {};
  if (map_key_input.value != '') {
    var paramPairs = map_key_input.value.split('&');
    for (var i = 0; i < paramPairs.length; i++) {
      var params = paramPairs[i].split('=');
      map_key[params[0]] = params[1];
    }
  }

  console.log("Setting map_key to", map_key);
  updateDataLayer();
}

function setMaskKey(input) {
  if (input != null) {
    mask_key_input.value = input;
  }

  mask_key = {};
  if (mask_key_input.value != '') {
    var paramPairs = mask_key_input.value.split('&');
    for (var i = 0; i < paramPairs.length; i++) {
      var params = paramPairs[i].split('=');
      mask_key[params[0]] = params[1];
    }
  }

  console.log("Setting mask_key to", mask_key);
  updateDataLayer();
}

var select_projection = document.getElementsByName('projection');
for (var i = 0; i < select_projection.length; i++) {
  select_projection[i].onchange = (function(e) {
    setProjection();
  });
}

var select_binning = document.getElementsByName('binning');
for (var i = 0; i < select_binning.length; i++) {
  select_binning[i].onchange = (function(e) {
    setBinning(e.target.value);
  });
  var slider = document.getElementById('binning_'+select_binning[i].value+'_size');
  if (slider) {
    slider.onchange = (function(e) {
      setBinning(e.target.value);
    });
  }
}

noUiSlider.create(year_slider, {
  start: [1600, new Date().getFullYear()],
  step: 1,
  connect: true,
  range: {
    'min': 1600,
    'max': new Date().getFullYear()
  }
});

year_slider.noUiSlider.on('update', function (vals) {
  // only adjust the range the user can see
  document.getElementById("years_text").innerText = Math.floor(vals[0]) + " - " + Math.floor(vals[1]);
});

year_slider.noUiSlider.on('change', function (vals) {
  // native JS works, while JQuery seems to have issue
  document.getElementById("year_range").checked = true;
  document.getElementById("year_all").checked = false;
  document.getElementById("years_text").innerText = Math.floor(vals[0]) + " - " + Math.floor(vals[1]);
  document.getElementById("year_range").value = Math.floor(vals[0]) + "," + Math.floor(vals[1]);
  setYears();
});

var select_year = document.getElementsByName('year');
for (var i = 0; i < select_year.length; i++) {
  select_year[i].onchange = (function(e) {
    setYears();
  });
}

var select_bor = document.getElementsByName('basis_of_record');
for (var i = 0; i < select_bor.length; i++) {
  select_bor[i].onchange = (function(e) {
    setBors();
  });
}

var select_datalayer = document.getElementsByName('datalayer');
for (var i = 0; i < select_datalayer.length; i++) {
  select_datalayer[i].onchange = (function(e) {
    setDataLayer();
  });
}

var select_source = document.getElementsByName('datasource');
for (var i = 0; i < select_source.length; i++) {
  select_source[i].onchange = (function(e) {
    setSource();
  });
}

var select_render = document.getElementsByName('render');
for (var i = 0; i < select_render.length; i++) {
  select_render[i].onchange = (function(e) {
    setRender();
  });
}

var select_raster_style = document.getElementsByName('raster_style');
for (var i = 0; i < select_raster_style.length; i++) {
  select_raster_style[i].onchange = (function(e) {
    setRasterStyle();
  });
}

var select_basemap_style = document.getElementsByName('basemap_style');
for (var i = 0; i < select_basemap_style.length; i++) {
  select_basemap_style[i].onchange = (function(e) {
    setBasemapStyle();
  });
}

var select_basemap_language = document.getElementById('basemap_language');
select_basemap_language.onchange = (function(e) {
  setBasemapStyle();
});

var select_show_grid = document.getElementsByName('tile_grid');
for (var i = 0; i < select_show_grid.length; i++) {
  select_show_grid[i].onchange = (function(e) {
    setGrid();
  });
}

var map_key_input = document.getElementById('map_key');
map_key_input.onchange = (function(e) {
  setMapKey();
});

var mask_key_input = document.getElementById('mask_key');
mask_key_input.onchange = (function(e) {
  setMaskKey();
});

var base_url_template_input = document.getElementById('base_url_template');
var data_url_template_input = document.getElementById('data_url_template');

setProjection();
setBors();
setMapKey();
setMaskKey();
setYears();
setBinning();
setRender();
setRasterStyle();
setBasemapStyle();
setSource();
initialized = true;
updateBaseLayer();
updateDataLayer();
