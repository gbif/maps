
var fragments = fragments || {}; // this will be injected at build time

var util = require('../../app/util/util'); // TODO: just remove them
var DOM = require('../../app/util/dom');
var mapbox = require('mapbox-gl');

var defaultMinZoom = 0;
var defaultMaxZoom = 20;
var defaultOptions = {
  center: [0,0],
  zoom: 0,
  minZoom: defaultMinZoom,
  maxZoom: defaultMaxZoom,
  renderMode: "png" // mode can be webgl or png
}

/**
 * The `Map` object represents the map on your page.
 * It exposes methods and properties that enable you to programmatically change the map.
 *
 * You create a `Map` by specifying a `container` and other options.
 * Then GBIF Map JS initializes the map on the page and returns your `Map` object.
 *
 * @class Map
 * @param {Object} options
 * @param {HTMLElement|string} options.container The HTML element in which Mapbox GL JS will render the map, or the element's string `id`.
 * @param {LngLatLike} [options.center=[0, 0]] The inital geographical centerpoint of the map.
 * @param {number} [options.minZoom=0] The minimum zoom level of the map (1-20).
 * @param {number} [options.minZoom=0] The minimum zoom level of the map (1-20).
 * @param {number} [options.maxZoom=20] The maximum zoom level of the map (1-20).
 * @param {number} [options.zoom=0] The initial zoom level of the map.
 * @param {string} [options.renderMode='png'] Where GBIF layers should use `png` or `webgl` rendering.
 * @example
 * var map = new gbif.Map({
 *   container: 'map',
 *   center: [-122.420679, 37.772537],
 *   zoom: 13,
 *   renderMode: png
 * });
 */
module.exports = {
  Map: function(options) {

    var self = this;

    // read the options setting defaults
    options = util.extend({}, defaultOptions, options);

    // split out container into 2: one for the map, and one for the GBIF pane
    var container = document.getElementById(options.container);
    var controlContainer = DOM.create('div', 'gbif-map__control-container', container);
    var controlContent = DOM.create('div', 'gbif-map__control-content', controlContainer);
    var mapContainer = DOM.create('div', 'gbif-map__map-container', container);

    var controlToggle = function toggleGBIFControls() {
      controlContainer.classList.toggle("open");
      if (!controlContainer.classList.contains("open")) {
        // required to catch times when the map has refreshed underneath the control widget
        self._map.resize();
      }
    }

    // create a button to toggle the container
    var controlToggleContainer = DOM.create('div', 'gbif-map__control-toggle-container', controlContainer);
    var controlToggleButton = _createButton('gbif-map__control-toggle-button', controlToggle, controlToggleContainer);


    var c = DOM.create('div', '', controlContent);
    c.innerHTML = fragments['mapControl'];
    //c.innerHTML = fs.readFileSync(path.join(__dirname, "../../generated-js/ui/controller_partial.html"));

    //c.innerHTML = fs.readFileSync("./generated-js/ui/controller_partial.html");

    //console.log(window.nunjucksPrecompiled["ui/controller_partial.njk"].root());
    //c.innerHTML = nunjucks.render("../../generated-js/ui/controller_partial.js");
    //c.innerHTML = nunjucks.render("../../generated-js/ui/controller_partial.js");

    // setup the map
    options.container = mapContainer;
    initialiseMapboxOptions(options);
    self._map = new mapbox.Map(options);
    self._map.addControl(new mapbox.Navigation({position: 'bottom-right'}));

    // A quick test - add an ALL data layer for now
    self._map.on('style.load', function () {
      self._map.addSource('tile', {
        type: 'raster',
        "tileSize": 256,
        //"tiles": ["http://localhost:3000//api/occurrence/density/{z}/{x}/{y}.png"]
        "tiles": ["http://maptest-vh.gbif.org:6081/index.html?z={z}&x={x}&y={y}"]
      });
      self._map.addLayer({
        "id": "simple",
        "type": "raster",
        "source": "tile",
      });
    });

    /**
     * Enhances the provided options with the required settings that mapbox needs.
     */
    function initialiseMapboxOptions(options) {
      options.style = {
        "version": 8,
        "sources": {},
        "layers": []
      }
    }

    // creates a button appemded to the container with a click event that will call the function.
    function _createButton (className, fn, container) {
      var a = DOM.create('button', className, container);
      a.type = 'button';
      a.addEventListener('click', function() { fn(); });
      return a;
    }
  }
};
