"use strict";

// GBIF Basemap layer definitions

class GBIFLayers {
	// srs
	// extent
	// tileGrid
	// resolutions

  baseRaster(srs, urlParams) {
    var l = GBIF_Layers[srs+'_Base_Raster']();
		return addParams(l, null, urlParams);
	}

	occurrenceVector(srs, source, urlParams) {
		var l = GBIF_Layers[srs+'_OccurrenceDensity_Vector']();
    return addParams(l, source, urlParams);
	}

	occurrenceRaster(srs, source, urlParams) {
		var l = GBIF_Layers[srs+'_OccurrenceDensity_Raster']();
    return addParams(l, source, urlParams);
	}

	grid(srs, urlParams) {
		return GBIF_Layers[srs+'_Grid'];
	}
}

var addParams = function(layer, source, urlParams) {
  var url = layer.getSource().getUrls()[0] + toParams(urlParams);
  url = url.slice(0, -1);
  url = url.replace('density', source);
  layer.getSource().setUrl(url);
  return layer;
}

var toParams = function(urlParams) {
  var urlParams = urlParams;
  var query = "";
  for (var key in urlParams) {
    query = query + key + '=' + urlParams[key] + '&';
  }
  return query;
}

var GBIF_Layers = [];

var gbifBaselayerBase = "https://tile.gbif.org";
var gbifOccurrenceLayerBase = "https://api.gbif.org/v2";
var gbifOccurrenceVectorLayerBase = gbifOccurrenceLayerBase;
var gbifOccurrenceRasterLayerBase = gbifOccurrenceLayerBase;
gbifOccurrenceVectorLayerBase = "../../..";
//gbifOccurrenceRasterLayerBase = "http://localhost:3000";

var densityColours = ["#FFFF00", "#FFCC00", "#FF9900", "#FF6600", "#FF3300", "#FF0000"];

var densityPoints = [
	new ol.style.Style({
		image: new ol.style.Circle({
			fill: new ol.style.Fill({color: densityColours[0]}),
			radius: 1
		}),
		fill: new ol.style.Fill({color: densityColours[0]})
	}),
	new ol.style.Style({
		image: new ol.style.Circle({
			fill: new ol.style.Fill({color: densityColours[1]}),
			radius: 1
		}),
		fill: new ol.style.Fill({color: densityColours[1]})
	}),
	new ol.style.Style({
		image: new ol.style.Circle({
			fill: new ol.style.Fill({color: densityColours[2]}),
			radius: 1
		}),
		fill: new ol.style.Fill({color: densityColours[2]})
	}),
	new ol.style.Style({
		image: new ol.style.Circle({
			fill: new ol.style.Fill({color: densityColours[3]}),
			radius: 1
		}),
		fill: new ol.style.Fill({color: densityColours[3]})
	}),
	new ol.style.Style({
		image: new ol.style.Circle({
			fill: new ol.style.Fill({color: densityColours[4]}),
			radius: 1
		}),
		fill: new ol.style.Fill({color: densityColours[4]})
	}),
	new ol.style.Style({
		image: new ol.style.Circle({
			fill: new ol.style.Fill({color: densityColours[5]}),
			radius: 1
		}),
		fill: new ol.style.Fill({color: densityColours[5]})
	}),
];

var densityHexagonsStrokeColour = '#999999';
var densityHexagonsStrokeWidth = 0.5;
var densityHexagons = [
	new ol.style.Style({
		stroke: new ol.style.Stroke({
			color: densityHexagonsStrokeColour,
			width: densityHexagonsStrokeWidth
		}),
		fill: new ol.style.Fill({color: densityColours[0]})
	}),
	new ol.style.Style({
		stroke: new ol.style.Stroke({
			color: densityHexagonsStrokeColour,
			width: densityHexagonsStrokeWidth
		}),
		fill: new ol.style.Fill({color: densityColours[1]})
	}),
	new ol.style.Style({
		stroke: new ol.style.Stroke({
			color: densityHexagonsStrokeColour,
			width: densityHexagonsStrokeWidth
		}),
		fill: new ol.style.Fill({color: densityColours[2]})
	}),
	new ol.style.Style({
		stroke: new ol.style.Stroke({
			color: densityHexagonsStrokeColour,
			width: densityHexagonsStrokeWidth
		}),
		fill: new ol.style.Fill({color: densityColours[3]})
	}),
	new ol.style.Style({
		stroke: new ol.style.Stroke({
			color: densityHexagonsStrokeColour,
			width: densityHexagonsStrokeWidth
		}),
		fill: new ol.style.Fill({color: densityColours[4]})
	}),
	new ol.style.Style({
		stroke: new ol.style.Stroke({
			color: densityHexagonsStrokeColour,
			width: densityHexagonsStrokeWidth
		}),
		fill: new ol.style.Fill({color: densityColours[5]})
	}),
];

var magnitude = function(total) {
	if (total <= 10) return 0;
	if (total <= 100) return 1;
	if (total <= 1000) return 2;
	if (total <= 10000) return 3;
	if (total <= 100000) return 4;
	return 5;
};

function densityStyle() {
	return function(feature, resolution) {
		var styles = [];
		var m = magnitude(feature.get('total'));
    var type = feature.getType();

    if (type == 'Point') {
			styles[0] = densityPoints[m];
    }
    else if (type == 'Polygon') {
      styles[0] = densityHexagons[m];
    }

		return styles;
	};
}

// 3857 — Web Mercator

GBIF_Layers['3857_OSM'] = new ol.layer.Tile({
	source: new ol.source.OSM({
		wrapX: false
	}),
});

GBIF_Layers['3857_Grid'] = new ol.layer.Tile({
	extent: ol.proj.get('EPSG:3857').getExtent(),
	source: new ol.source.TileDebug({
		projection: 'EPSG:3857',
 		tileGrid: ol.tilegrid.createXYZ({
			maxZoom: 16,
			tileSize: [512, 512]
		}),
		tilePixelRatio: 8,
		wrapX: false
	})
});

GBIF_Layers['3857_Base_Vector'] = function() { return new ol.layer.VectorTile({
  source: new ol.source.VectorTile({
		projection: 'EPSG:3857',
		format: new ol.format.MVT(),
 		tileGrid: ol.tilegrid.createXYZ({
			maxZoom: 14,
			tileSize: [512, 512]
		}),
		tilePixelRatio: 8,
		url: gbifBaselayerBase + '/3857/omt/{z}/{x}/{y}.pbf?',
		wrapX: false
  }),
  //style: createStyle(),
})};

GBIF_Layers['3857_Base_Raster'] = function() { return new ol.layer.Tile({
	source: new ol.source.TileImage({
		projection: 'EPSG:3857',
 		tileGrid: ol.tilegrid.createXYZ({
			maxZoom: 14,
			tileSize: [512, 512]
		}),
		tilePixelRatio: 2,
		url: gbifBaselayerBase + '/3857/omt/{z}/{x}/{y}@2x.png?',
		wrapX: true
	}),
})};

GBIF_Layers['3857_OccurrenceDensity_Vector'] = function() { return new ol.layer.VectorTile({
	renderMode: 'image',
  source: new ol.source.VectorTile({
		format: new ol.format.MVT(),
		projection: 'EPSG:3857',
 		tileGrid: ol.tilegrid.createXYZ({
			maxZoom: 16,
			tileSize: 512,
		}),
		tilePixelRatio: 8,
		url: gbifOccurrenceVectorLayerBase + '/map/occurrence/density/{z}/{x}/{y}.mvt?srs=EPSG:3857&',
		wrapX: false
  }),
  style: densityStyle()
})};

GBIF_Layers['3857_OccurrenceDensity_Raster'] = function() { return new ol.layer.Tile({
  source: new ol.source.TileImage({
		projection: 'EPSG:3857',
 		tileGrid: ol.tilegrid.createXYZ({
			maxZoom: 16,
			tileSize: [512, 512]
		}),
		tilePixelRatio: 2,
		url: gbifOccurrenceRasterLayerBase + '/map/occurrence/density/{z}/{x}/{y}@2x.png?srs=EPSG:3857&',
		wrapX: true
  }),
})};

// 4326 — WGS84 Equirectangular / Plate Careé

GBIF_Layers['4326_ESRI'] = new ol.layer.Tile({
	extent: GBIF_4326.extent(),
	source: new ol.source.TileImage({
		projection: GBIF_4326.projection(),
	  tileGrid: GBIF_4326.tileGrid(),
		// Or https://services.arcgisonline.com/arcgis/rest/services/ESRI_Imagery_World_2D/MapServer/tile/0/0/1
		url: 'http://server.arcgisonline.com/arcgis/rest/services/ESRI_StreetMap_World_2D/MapServer/tile/{z}/{y}/{x}?',
		// From http://server.arcgisonline.com/ArcGIS/rest/services/ESRI_StreetMap_World_2D/MapServer
		wrapX: false
	}),
});

GBIF_Layers['4326_Grid'] = new ol.layer.Tile({
	extent: GBIF_4326.extent(),
	source: new ol.source.TileDebug({
		projection: GBIF_4326.projection(),
	  tileGrid: GBIF_4326.tileGrid(),
		wrapX: false
	})
});

GBIF_Layers['4326_Base_Vector'] = function() { return new ol.layer.VectorTile({
	extent: GBIF_4326.extent(),
  source: new ol.source.VectorTile({
		projection: GBIF_4326.projection(),
	  tileGrid: GBIF_4326.tileGrid(),
		format: new ol.format.MVT(),
		tilePixelRatio: 8,
		// 512 ratio 1
		// 1024 ratio 2
		// 2048 ratio 3
		// 4096 ratio 4
		url: gbifBaselayerBase + '/4326/omt/{z}/{x}/{y}.pbf?',
		wrapX: false
  }),
  //style: createStyle(),
})};

GBIF_Layers['4326_Base_Raster'] = function() { return new ol.layer.Tile({
	//extent: GBIF_4326.extent(),
	source: new ol.source.TileImage({
		projection: GBIF_4326.projection(),
	  tileGrid: GBIF_4326.tileGrid(),
		tilePixelRatio: 2,
		url: gbifBaselayerBase + '/4326/omt/{z}/{x}/{y}@2x.png?',
		wrapX: true
	}),
})};

GBIF_Layers['4326_OccurrenceDensity_Vector'] = function() { return new ol.layer.VectorTile({
	//extent: GBIF_4326.extent(),
	renderMode: 'image',
  source: new ol.source.VectorTile({
		projection: GBIF_4326.projection(),
	  tileGrid: GBIF_4326.tileGrid(),
		tilePixelRatio: 8,
		format: new ol.format.MVT(),
		url: gbifOccurrenceVectorLayerBase + '/map/occurrence/density/{z}/{x}/{y}.mvt?srs=EPSG:4326&',
    wrapX: false,
  }),
  style: densityStyle()
})};

GBIF_Layers['4326_OccurrenceDensity_Raster'] = function() { return new ol.layer.Tile({
	//extent: GBIF_4326.extent(),
  source: new ol.source.TileImage({
		projection: GBIF_4326.projection(),
	  tileGrid: GBIF_4326.tileGrid(),
		tilePixelRatio: 2,
		url: gbifOccurrenceRasterLayerBase + '/map/occurrence/density/{z}/{x}/{y}@2x.png?srs=EPSG:4326&',
		wrapX: true
  }),
	visible: true
})};

// 3575 — Arctic LAEA

var amHalfWidth = 12742014.4;
GBIF_Layers['3575_AlaskaMapped'] = new ol.layer.Tile({
  source: new ol.source.TileImage({
    projection: 'EPSG:3575',
    tileGrid: new ol.tilegrid.TileGrid({
			extent: [-amHalfWidth, -amHalfWidth, amHalfWidth, amHalfWidth],
			origin: [0,0],
			minZoom: 0,
			maxZoom: 8,
			resolutions: Array.from(new Array(9), (x,i) => (amHalfWidth/(512*Math.pow(2,i-1)))),
			tileSize: [512, 512]
		}),
		tileUrlFunction: function (tileCoord, pixelRatio, projection) {
			if (tileCoord === null) return undefined;
			var z = tileCoord[0];
			var x = tileCoord[1];
			var y = tileCoord[2];
			return 'https://download.gbif.org/MapDataMirror/AlaskaMapped/ac-3575/' + z + '/' + x + '/' + y + '@2x.png';
		},
		//url: '//download.gbif.org/MapDataMirror/AlaskaMapped/ac-3575/{z}/{x}/{-y}@2x.png',
    tilePixelRatio: 1,
  }),
});

GBIF_Layers['3575_Grid'] = new ol.layer.Tile({
  extent: GBIF_3575.extent(),
  source: new ol.source.TileDebug({
    projection: GBIF_3575.projection(),
    tileGrid: GBIF_3575.tileGrid(),
  }),
});

GBIF_Layers['3575_Base_Vector'] = function() { return new ol.layer.VectorTile({
  extent: GBIF_3575.extent(),
  source: new ol.source.VectorTile({
    projection: GBIF_3575.projection(),
    format: new ol.format.MVT(),
    tileGrid: GBIF_3575.tileGrid(),
		url: gbifBaselayerBase + '/3575/omt/{z}/{x}/{y}.mvt?',
    tilePixelRatio: 8,
  }),
  style: densityStyle()
})};

GBIF_Layers['3575_Base_Raster'] = function() { return new ol.layer.Tile({
  extent: GBIF_3575.extent(),
	source: new ol.source.TileImage({
    projection: GBIF_3575.projection(),
    tileGrid: GBIF_3575.tileGrid(),
		url: gbifBaselayerBase + '/3575/omt/{z}/{x}/{y}@2x.png?',
		tilePixelRatio: 2,
	}),
})};

GBIF_Layers['3575_OccurrenceDensity_Vector'] = function() { return new ol.layer.VectorTile({
	extent: GBIF_3575.extent(),
	renderMode: 'image',
  source: new ol.source.VectorTile({
		projection: GBIF_3575.projection(),
	  tileGrid: GBIF_3575.tileGrid(),
		tilePixelRatio: 8,
		format: new ol.format.MVT(),
		url: gbifOccurrenceVectorLayerBase + '/map/occurrence/density/{z}/{x}/{y}.mvt?srs=EPSG:3575&',
  }),
  style: densityStyle()
})};

GBIF_Layers['3575_OccurrenceDensity_Raster'] = function() { return new ol.layer.Tile({
  extent: GBIF_3575.extent(),
  source: new ol.source.TileImage({
    projection: GBIF_3575.projection(),
    tileGrid: GBIF_3575.tileGrid(),
		tilePixelRatio: 2,
		url: gbifOccurrenceRasterLayerBase + '/map/occurrence/density/{z}/{x}/{y}@2x.png?srs=EPSG:3575&',
  }),
})};

// 3031 — Antarctic Stereographic

GBIF_Layers['3031_PolarView'] = new ol.layer.Tile({
	extent: GBIF_3031.extent(),
  source: new ol.source.TileWMS({
		url: 'http://geos.polarview.aq/geoserver/wms',
		params: {'GBIF_LAYERS': 'polarview:MODIS_Terra_Antarctica', 'TILED': true},
		serverType: 'geoserver',
		tileGrid: GBIF_3031.tileGrid(),
  })
});

GBIF_Layers['3031_Grid'] = new ol.layer.Tile({
  extent: GBIF_3031.extent(),
  source: new ol.source.TileDebug({
    projection: GBIF_3031.projection(),
    tileGrid: GBIF_3031.tileGrid(),
  }),
});

GBIF_Layers['3031_Base_Vector'] = function() { return new ol.layer.VectorTile({
  extent: GBIF_3031.extent(),
  source: new ol.source.VectorTile({
    projection: GBIF_3031.projection(),
    format: new ol.format.MVT(),
    tileGrid: GBIF_3031.tileGrid(),
		url: gbifBaselayerBase + '/3031/omt/{z}/{x}/{y}.mvt?',
    tilePixelRatio: 8,
  }),
  //style: densityStyle()
})};

GBIF_Layers['3031_Base_Raster'] = function() { return new ol.layer.Tile({
  extent: GBIF_3031.extent(),
	source: new ol.source.TileImage({
		projection: 'EPSG:3031',
	  tileGrid: GBIF_3031.tileGrid(),
		url: gbifBaselayerBase + '/3031/omt/{z}/{x}/{y}@2x.png?',
		tilePixelRatio: 2,
	}),
})};

GBIF_Layers['3031_OccurrenceDensity_Vector'] = function() { return new ol.layer.VectorTile({
	extent: GBIF_3031.extent(),
	renderMode: 'image',
  source: new ol.source.VectorTile({
		projection: GBIF_3031.projection(),
	  tileGrid: GBIF_3031.tileGrid(),
		tilePixelRatio: 8,
		format: new ol.format.MVT(),
		url: gbifOccurrenceVectorLayerBase + '/map/occurrence/density/{z}/{x}/{y}.mvt?srs=EPSG:3031&',
  }),
  style: densityStyle()
})};

GBIF_Layers['3031_OccurrenceDensity_Raster'] = function() { return new ol.layer.Tile({
	extent: GBIF_3031.extent(),
  source: new ol.source.TileImage({
    projection: GBIF_3031.projection(),
		tileGrid: GBIF_3031.tileGrid(),
		tilePixelRatio: 2,
		url: gbifOccurrenceRasterLayerBase + '/map/occurrence/density/{z}/{x}/{y}@2x.png?srs=EPSG:3031&',
  }),
})};
