// GBIF Map base layer definitions, for OpenLayers.

var GBIF_Layers = window.GBIF_Layers;

var layers = {
	"3857": {
		"base": GBIF_Layers['3857_Base_Raster'],
		"occurrence_vector": GBIF_Layers['3857_OccurrenceDensity_Vector'],
		"occurrence_raster": GBIF_Layers['3857_OccurrenceDensity_Raster'],
		"grid": GBIF_Layers['3857_Grid'],
	},

	"4326": {
		"base": GBIF_Layers['4326_Base_Raster'],
		"occurrence_vector": GBIF_Layers['4326_OccurrenceDensity_Vector'],
		"occurrenceadhoc_vector": GBIF_Layers['4326_OccurrenceDensityAdhoc_Vector'],
		"occurrence_raster": GBIF_Layers['4326_OccurrenceDensity_Raster'],
		"grid": GBIF_Layers['4326_Grid'],
	},

	"3575": {
		"base": GBIF_Layers['3575_Base_Raster'],
		"occurrence_vector": GBIF_Layers['3575_OccurrenceDensity_Vector'],
		"occurrence_raster": GBIF_Layers['3575_OccurrenceDensity_Raster'],
		"grid": GBIF_Layers['3575_Grid'],
	},

	"3031": {
		"base": GBIF_Layers['3031_Base_Raster'],
		"occurrence_vector": GBIF_Layers['3031_OccurrenceDensity_Vector'],
		"occurrence_raster": GBIF_Layers['3031_OccurrenceDensity_Raster'],
		"grid": GBIF_Layers['3031_Grid'],
	}
};

// TODO: Tidy up.
function createStatsStyle() {
  var fill = new ol.style.Fill({color: '#000000'});
  var stroke = new ol.style.Stroke({color: '#000000', width: 1});
  var text = new ol.style.Text({
    text: 'XYXYXY',
    fill: fill,
    stroke: stroke,
    font: '16px "Open Sans", "Arial Unicode MS"'
  });

  var styles = [];
  return function (feature, resolution) {
    var length = 0;
    //console.log(feature);
    text.setText('Occurrences: ' + feature.get('total'));
    //console.log(feature.get('total'));
    styles[length++] = new ol.style.Style({
      stroke: new ol.style.Stroke({color: '#000000'}),
      text: text
    });
    styles.length = length;
    return styles;
  };
}
var statsLayer = new ol.layer.Vector({
  source: new ol.source.Vector({
    url: '/map/occurrence/density/.json',
    format: new ol.format.GeoJSON()
  }),
  style: createStatsStyle()
});

var views = {
	"3857": new ol.View({
		center: ol.proj.fromLonLat([0, 0], 'EPSG:3857'),
                projection: ol.proj.get('EPSG:3857'),
                zoom: 1,
        }),
	"4326": new ol.View({
		center: [0, 0],
                projection: ol.proj.get('EPSG:4326'),
                zoom: 1,
        }),
	"3575": new ol.View({
		center: [0, 0],
                projection: ol.proj.get('EPSG:3575'),
                zoom: 1,
        }),
	"3031": new ol.View({
		center: [0, 0],
                extent: ol.proj.get('EPSG:3031').getExtent(),
                projection: ol.proj.get('EPSG:3031'),
                zoom: 1,
        }),
};

var map;

var currentLayers = [];

function bindInputs(layerid) {
        var visibilityInput = document.getElementById(layerid + '_visible');
        visibilityInput.onchange = (function() {
		currentLayers[layerid].setVisible(this.checked);
        });

        var opacityInput = document.getElementById(layerid + '_opacity');
        opacityInput.oninput = (function() {
		currentLayers[layerid].setOpacity(parseFloat(this.value));
        });

	var layer = currentLayers[layerid];
        visibilityInput.checked = layer.getVisible() ? 'on' : '';
        opacityInput.value = (String(layer.getOpacity()));
}

function setProjection(srs) {
	console.log("Setting projection to "+srs);

	currentLayers['base'] = layers[srs]['base'];
	currentLayers['occurrence_vector'] = layers[srs]['occurrence_vector'];
//	currentLayers['occurrenceadhoc_vector'] = layers[srs]['occurrenceadhoc_vector'];
	currentLayers['occurrence_raster'] = layers[srs]['occurrence_raster'];
	currentLayers['grid'] = layers[srs]['grid'];

	l = [];
	l.push(currentLayers['base']);

	l.push(currentLayers['occurrence_vector']);
//	l.push(currentLayers['occurrenceadhoc_vector']);
//	l.push(currentLayers['occurrence_raster']);
	l.push(currentLayers['grid']);
	l.push(statsLayer);

	map.getLayers().clear();
	map.getLayers().extend(l);
	map.setView(views[srs]);

	bindInputs('grid');
}

map = new ol.Map({
	target: 'map',
});

var select_projection = document.getElementsByName('projection');
for (var i = 0; i < select_projection.length; i++) {
	if (select_projection[i].checked) {
		setProjection(select_projection[i].value);
	}
	select_projection[i].onchange = (function(e) {
		setProjection(e.target.value);
	});
}
