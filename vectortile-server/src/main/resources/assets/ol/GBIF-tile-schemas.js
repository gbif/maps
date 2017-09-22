"use strict";

//proj4.defs("EPSG:3575", "+proj=laea +lat_0=90 +lon_0=10 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs");
//proj4.defs("EPSG:4326", "+proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees");
//proj4.defs("EPSG:3575", "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs");

// GBIF Tile Schemas for all four supported projections.
////////////////////////////////////////////////////////

class GBIFTiles {
	// srs
	// extent
	// tileGrid
	// resolutions

	constructor(config) {
		this.config = config;
		proj4.defs(config.projection, config.proj4)
		ol.proj.get(config.projection).setExtent(config.extent);
	}

	projection() {
		return this.config.projection;
	}

	extent() {
		return ol.proj.get(this.config.projection).getExtent();
	}

	tileGrid() {
		return new ol.tilegrid.TileGrid({
			extent: ol.proj.get(this.config.projection).getExtent(),
			origin: this.config.gridOrigin,
			minZoom: this.config.minZoom,
			maxZoom: this.config.maxZoom,
			resolutions: this.config.resolutions,
			tileSize: this.config.tileSize,
		});
	}
}

var tileSize = 512;

// 3857 — Web Mercator
// (The default)

// 4326 — WGS84 Equirectangular / Plate Careé
var GBIF_4326 = new GBIFTiles({
	projection: "EPSG:4326",
	proj4: "+proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees",
	extent: [-180.0, -90.0, 180.0, 90.0],
	worldExtent: [-180.0, -90.0, 180.0, 90.0],
	tileSize: tileSize,
	resolutions: Array(17).fill().map((_, i) => ( 180.0 / tileSize / Math.pow(2, i) )),
	gridOrigin: [-180,90],
	minZoom: 0,
	maxZoom: 16
});

// 3575 — Antarctic LAEA
var laeaHalfWidth = Math.sqrt(2) * 6371007.2;
var GBIF_3575 = new GBIFTiles({
	projection: "EPSG:3575",
	proj4: "+proj=laea +lat_0=90 +lon_0=10 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
	extent: [-laeaHalfWidth, -laeaHalfWidth, laeaHalfWidth, laeaHalfWidth],
	tileSize: tileSize,
	resolutions: Array(17).fill().map((_, i) => ( laeaHalfWidth/(tileSize*Math.pow(2,i-1)) )),
	gridOrigin: [-laeaHalfWidth,laeaHalfWidth],
  minZoom: 0,
  maxZoom: 16
});

// 3031 — Antarctic Stereographic
var stereHalfWidth = 12367396.2185; // To the Equator
var GBIF_3031 = new GBIFTiles({
	projection: "EPSG:3031",
	proj4: "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs",
	extent: [-stereHalfWidth, -stereHalfWidth, stereHalfWidth, stereHalfWidth],
	tileSize: tileSize,
	resolutions: Array(17).fill().map((_, i) => ( stereHalfWidth/(tileSize*Math.pow(2,i-1)) )),
	gridOrigin: [-stereHalfWidth,stereHalfWidth],
  minZoom: 0,
  maxZoom: 16
});
