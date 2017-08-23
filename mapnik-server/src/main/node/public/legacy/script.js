(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
(function (global){
; var __browserify_shim_require__=require;(function browserifyShim(module, exports, require, define, browserify_shim__define__module__export__) {
/*!
 * Modest Maps JS v3.3.6
 * http://modestmaps.com/
 *
 * Copyright (c) 2011 Stamen Design, All Rights Reserved.
 *
 * Open source under the BSD License.
 * http://creativecommons.org/licenses/BSD/
 *
 * Versioned using Semantic Versioning (v.major.minor.patch)
 * See CHANGELOG and http://semver.org/ for more details.
 *
 */

var previousMM = MM;

// namespacing for backwards-compatibility
if (!com) {
    var com = {};
    if (!com.modestmaps) com.modestmaps = {};
}

var MM = com.modestmaps = {
  noConflict: function() {
    MM = previousMM;
    return this;
  }
};

(function(MM) {
    // Make inheritance bearable: clone one level of properties
    MM.extend = function(child, parent) {
        for (var property in parent.prototype) {
            if (typeof child.prototype[property] == "undefined") {
                child.prototype[property] = parent.prototype[property];
            }
        }
        return child;
    };

    MM.getFrame = function () {
        // native animation frames
        // http://webstuff.nfshost.com/anim-timing/Overview.html
        // http://dev.chromium.org/developers/design-documents/requestanimationframe-implementation
        // http://paulirish.com/2011/requestanimationframe-for-smart-animating/
        // can't apply these directly to MM because Chrome needs window
        // to own webkitRequestAnimationFrame (for example)
        // perhaps we should namespace an alias onto window instead? 
        // e.g. window.mmRequestAnimationFrame?
        return function(callback) {
            (window.requestAnimationFrame  ||
            window.webkitRequestAnimationFrame ||
            window.mozRequestAnimationFrame    ||
            window.oRequestAnimationFrame      ||
            window.msRequestAnimationFrame     ||
            function (callback) {
                window.setTimeout(function () {
                    callback(+new Date());
                }, 10);
            })(callback);
        };
    }();

    // Inspired by LeafletJS
    MM.transformProperty = (function(props) {
        if (!this.document) return; // node.js safety
        var style = document.documentElement.style;
        for (var i = 0; i < props.length; i++) {
            if (props[i] in style) {
                return props[i];
            }
        }
        return false;
    })(['transform', 'WebkitTransform', 'OTransform', 'MozTransform', 'msTransform']);

    MM.matrixString = function(point) {
        // Make the result of point.scale * point.width a whole number.
        if (point.scale * point.width % 1) {
            point.scale += (1 - point.scale * point.width % 1) / point.width;
        }

        var scale = point.scale || 1;
        if (MM._browser.webkit3d) {
            return  'translate3d(' +
                point.x.toFixed(0) + 'px,' + point.y.toFixed(0) + 'px, 0px)' +
                'scale3d(' + scale + ',' + scale + ', 1)';
        } else {
            return  'translate(' +
                point.x.toFixed(6) + 'px,' + point.y.toFixed(6) + 'px)' +
                'scale(' + scale + ',' + scale + ')';
        }
    };

    MM._browser = (function(window) {
        return {
            webkit: ('WebKitCSSMatrix' in window),
            webkit3d: ('WebKitCSSMatrix' in window) && ('m11' in new WebKitCSSMatrix())
        };
    })(this); // use this for node.js global

    MM.moveElement = function(el, point) {
        if (MM.transformProperty) {
            // Optimize for identity transforms, where you don't actually
            // need to change this element's string. Browsers can optimize for
            // the .style.left case but not for this CSS case.
            if (!point.scale) point.scale = 1;
            if (!point.width) point.width = 0;
            if (!point.height) point.height = 0;
            var ms = MM.matrixString(point);
            if (el[MM.transformProperty] !== ms) {
                el.style[MM.transformProperty] =
                    el[MM.transformProperty] = ms;
            }
        } else {
            el.style.left = point.x + 'px';
            el.style.top = point.y + 'px';
            // Don't set width unless asked to: this is performance-intensive
            // and not always necessary
            if (point.width && point.height && point.scale) {
                el.style.width =  Math.ceil(point.width  * point.scale) + 'px';
                el.style.height = Math.ceil(point.height * point.scale) + 'px';
            }
        }
    };

    // Events
    // Cancel an event: prevent it from bubbling
    MM.cancelEvent = function(e) {
        // there's more than one way to skin this cat
        e.cancelBubble = true;
        e.cancel = true;
        e.returnValue = false;
        if (e.stopPropagation) { e.stopPropagation(); }
        if (e.preventDefault) { e.preventDefault(); }
        return false;
    };

    MM.coerceLayer = function(layerish) {
        if (typeof layerish == 'string') {
            // Probably a template string
            return new MM.Layer(new MM.TemplatedLayer(layerish));
        } else if ('draw' in layerish && typeof layerish.draw == 'function') {
            // good enough, though we should probably enforce .parent and .destroy() too
            return layerish;
        } else {
            // probably a MapProvider
            return new MM.Layer(layerish);
        }
    };

    // see http://ejohn.org/apps/jselect/event.html for the originals
    MM.addEvent = function(obj, type, fn) {
        if (obj.addEventListener) {
            obj.addEventListener(type, fn, false);
            if (type == 'mousewheel') {
                obj.addEventListener('DOMMouseScroll', fn, false);
            }
        } else if (obj.attachEvent) {
            obj['e'+type+fn] = fn;
            obj[type+fn] = function(){ obj['e'+type+fn](window.event); };
            obj.attachEvent('on'+type, obj[type+fn]);
        }
    };

    MM.removeEvent = function( obj, type, fn ) {
        if (obj.removeEventListener) {
            obj.removeEventListener(type, fn, false);
            if (type == 'mousewheel') {
                obj.removeEventListener('DOMMouseScroll', fn, false);
            }
        } else if (obj.detachEvent) {
            obj.detachEvent('on'+type, obj[type+fn]);
            obj[type+fn] = null;
        }
    };

    // Cross-browser function to get current element style property
    MM.getStyle = function(el,styleProp) {
        if (el.currentStyle)
            return el.currentStyle[styleProp];
        else if (window.getComputedStyle)
            return document.defaultView.getComputedStyle(el,null).getPropertyValue(styleProp);
    };
    // Point
    MM.Point = function(x, y) {
        this.x = parseFloat(x);
        this.y = parseFloat(y);
    };

    MM.Point.prototype = {
        x: 0,
        y: 0,
        toString: function() {
            return "(" + this.x.toFixed(3) + ", " + this.y.toFixed(3) + ")";
        },
        copy: function() {
            return new MM.Point(this.x, this.y);
        }
    };

    // Get the euclidean distance between two points
    MM.Point.distance = function(p1, p2) {
        return Math.sqrt(
            Math.pow(p2.x - p1.x, 2) +
            Math.pow(p2.y - p1.y, 2));
    };

    // Get a point between two other points, biased by `t`.
    MM.Point.interpolate = function(p1, p2, t) {
        return new MM.Point(
            p1.x + (p2.x - p1.x) * t,
            p1.y + (p2.y - p1.y) * t);
    };
    // Coordinate
    // ----------
    // An object representing a tile position, at as specified zoom level.
    // This is not necessarily a precise tile - `row`, `column`, and
    // `zoom` can be floating-point numbers, and the `container()` function
    // can be used to find the actual tile that contains the point.
    MM.Coordinate = function(row, column, zoom) {
        this.row = row;
        this.column = column;
        this.zoom = zoom;
    };

    MM.Coordinate.prototype = {

        row: 0,
        column: 0,
        zoom: 0,

        toString: function() {
            return "("  + this.row.toFixed(3) +
                   ", " + this.column.toFixed(3) +
                   " @" + this.zoom.toFixed(3) + ")";
        },
        // Quickly generate a string representation of this coordinate to
        // index it in hashes. 
        toKey: function() {
            // We've tried to use efficient hash functions here before but we took
            // them out. Contributions welcome but watch out for collisions when the
            // row or column are negative and check thoroughly (exhaustively) before
            // committing.
            return this.zoom + ',' + this.row + ',' + this.column;
        },
        // Clone this object.
        copy: function() {
            return new MM.Coordinate(this.row, this.column, this.zoom);
        },
        // Get the actual, rounded-number tile that contains this point.
        container: function() {
            // using floor here (not parseInt, ~~) because we want -0.56 --> -1
            return new MM.Coordinate(Math.floor(this.row),
                                     Math.floor(this.column),
                                     Math.floor(this.zoom));
        },
        // Recalculate this Coordinate at a different zoom level and return the
        // new object.
        zoomTo: function(destination) {
            var power = Math.pow(2, destination - this.zoom);
            return new MM.Coordinate(this.row * power,
                                     this.column * power,
                                     destination);
        },
        // Recalculate this Coordinate at a different relative zoom level and return the
        // new object.
        zoomBy: function(distance) {
            var power = Math.pow(2, distance);
            return new MM.Coordinate(this.row * power,
                                     this.column * power,
                                     this.zoom + distance);
        },
        // Move this coordinate up by `dist` coordinates
        up: function(dist) {
            if (dist === undefined) dist = 1;
            return new MM.Coordinate(this.row - dist, this.column, this.zoom);
        },
        // Move this coordinate right by `dist` coordinates
        right: function(dist) {
            if (dist === undefined) dist = 1;
            return new MM.Coordinate(this.row, this.column + dist, this.zoom);
        },
        // Move this coordinate down by `dist` coordinates
        down: function(dist) {
            if (dist === undefined) dist = 1;
            return new MM.Coordinate(this.row + dist, this.column, this.zoom);
        },
        // Move this coordinate left by `dist` coordinates
        left: function(dist) {
            if (dist === undefined) dist = 1;
            return new MM.Coordinate(this.row, this.column - dist, this.zoom);
        }
    };
    // Location
    // --------
    MM.Location = function(lat, lon) {
        this.lat = parseFloat(lat);
        this.lon = parseFloat(lon);
    };

    MM.Location.prototype = {
        lat: 0,
        lon: 0,
        toString: function() {
            return "(" + this.lat.toFixed(3) + ", " + this.lon.toFixed(3) + ")";
        },
        copy: function() {
            return new MM.Location(this.lat, this.lon);
        }
    };

    // returns approximate distance between start and end locations
    //
    // default unit is meters
    //
    // you can specify different units by optionally providing the
    // earth's radius in the units you desire
    //
    // Default is 6,378,000 metres, suggested values are:
    //
    // * 3963.1 statute miles
    // * 3443.9 nautical miles
    // * 6378 km
    //
    // see [Formula and code for calculating distance based on two lat/lon locations](http://jan.ucc.nau.edu/~cvm/latlon_formula.html)
    MM.Location.distance = function(l1, l2, r) {
        if (!r) {
            // default to meters
            r = 6378000;
        }
        var deg2rad = Math.PI / 180.0,
            a1 = l1.lat * deg2rad,
            b1 = l1.lon * deg2rad,
            a2 = l2.lat * deg2rad,
            b2 = l2.lon * deg2rad,
            c = Math.cos(a1) * Math.cos(b1) * Math.cos(a2) * Math.cos(b2),
            d = Math.cos(a1) * Math.sin(b1) * Math.cos(a2) * Math.sin(b2),
            e = Math.sin(a1) * Math.sin(a2);
        return Math.acos(c + d + e) * r;
    };

    // Interpolates along a great circle, f between 0 and 1
    //
    // * FIXME: could be heavily optimized (lots of trig calls to cache)
    // * FIXME: could be inmproved for calculating a full path
    MM.Location.interpolate = function(l1, l2, f) {
        if (l1.lat === l2.lat && l1.lon === l2.lon) {
            return new MM.Location(l1.lat, l1.lon);
        }
        var deg2rad = Math.PI / 180.0,
            lat1 = l1.lat * deg2rad,
            lon1 = l1.lon * deg2rad,
            lat2 = l2.lat * deg2rad,
            lon2 = l2.lon * deg2rad;

        var d = 2 * Math.asin(
            Math.sqrt(
              Math.pow(Math.sin((lat1 - lat2) * 0.5), 2) +
              Math.cos(lat1) * Math.cos(lat2) *
              Math.pow(Math.sin((lon1 - lon2) * 0.5), 2)));

        var A = Math.sin((1-f)*d)/Math.sin(d);
        var B = Math.sin(f*d)/Math.sin(d);
        var x = A * Math.cos(lat1) * Math.cos(lon1) +
          B * Math.cos(lat2) * Math.cos(lon2);
        var y = A * Math.cos(lat1) * Math.sin(lon1) +
          B * Math.cos(lat2) * Math.sin(lon2);
        var z = A * Math.sin(lat1) + B * Math.sin(lat2);

        var latN = Math.atan2(z, Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2)));
        var lonN = Math.atan2(y,x);

        return new MM.Location(latN / deg2rad, lonN / deg2rad);
    };
    
    // Returns bearing from one point to another
    //
    // * FIXME: bearing is not constant along significant great circle arcs.
    MM.Location.bearing = function(l1, l2) {
        var deg2rad = Math.PI / 180.0,
            lat1 = l1.lat * deg2rad,
            lon1 = l1.lon * deg2rad,
            lat2 = l2.lat * deg2rad,
            lon2 = l2.lon * deg2rad;
        var result = Math.atan2(
            Math.sin(lon1 - lon2) *
            Math.cos(lat2),
            Math.cos(lat1) *
            Math.sin(lat2) -
            Math.sin(lat1) *
            Math.cos(lat2) *
            Math.cos(lon1 - lon2)
        )  / -(Math.PI / 180);

        // map it into 0-360 range
        return (result < 0) ? result + 360 : result;
    };
    // Extent
    // ----------
    // An object representing a map's rectangular extent, defined by its north,
    // south, east and west bounds.

    MM.Extent = function(north, west, south, east) {
        if (north instanceof MM.Location &&
            west instanceof MM.Location) {
            var northwest = north,
                southeast = west;

            north = northwest.lat;
            west = northwest.lon;
            south = southeast.lat;
            east = southeast.lon;
        }
        if (isNaN(south)) south = north;
        if (isNaN(east)) east = west;
        this.north = Math.max(north, south);
        this.south = Math.min(north, south);
        this.east = Math.max(east, west);
        this.west = Math.min(east, west);
    };

    MM.Extent.prototype = {
        // boundary attributes
        north: 0,
        south: 0,
        east: 0,
        west: 0,

        copy: function() {
            return new MM.Extent(this.north, this.west, this.south, this.east);
        },

        toString: function(precision) {
            if (isNaN(precision)) precision = 3;
            return [
                this.north.toFixed(precision),
                this.west.toFixed(precision),
                this.south.toFixed(precision),
                this.east.toFixed(precision)
            ].join(", ");
        },

        // getters for the corner locations
        northWest: function() {
            return new MM.Location(this.north, this.west);
        },
        southEast: function() {
            return new MM.Location(this.south, this.east);
        },
        northEast: function() {
            return new MM.Location(this.north, this.east);
        },
        southWest: function() {
            return new MM.Location(this.south, this.west);
        },
        // getter for the center location
        center: function() {
            return new MM.Location(
                this.south + (this.north - this.south) * 0.5,
                this.east + (this.west - this.east) * 0.5
            );
        },

        // extend the bounds to include a location's latitude and longitude
        encloseLocation: function(loc) {
            if (loc.lat > this.north) this.north = loc.lat;
            if (loc.lat < this.south) this.south = loc.lat;
            if (loc.lon > this.east) this.east = loc.lon;
            if (loc.lon < this.west) this.west = loc.lon;
        },

        // extend the bounds to include multiple locations
        encloseLocations: function(locations) {
            var len = locations.length;
            for (var i = 0; i < len; i++) {
                this.encloseLocation(locations[i]);
            }
        },

        // reset bounds from a list of locations
        setFromLocations: function(locations) {
            var len = locations.length,
                first = locations[0];
            this.north = this.south = first.lat;
            this.east = this.west = first.lon;
            for (var i = 1; i < len; i++) {
                this.encloseLocation(locations[i]);
            }
        },

        // extend the bounds to include another extent
        encloseExtent: function(extent) {
            if (extent.north > this.north) this.north = extent.north;
            if (extent.south < this.south) this.south = extent.south;
            if (extent.east > this.east) this.east = extent.east;
            if (extent.west < this.west) this.west = extent.west;
        },

        // determine if a location is within this extent
        containsLocation: function(loc) {
            return loc.lat >= this.south &&
                loc.lat <= this.north &&
                loc.lon >= this.west &&
                loc.lon <= this.east;
        },

        // turn an extent into an array of locations containing its northwest
        // and southeast corners (used in MM.Map.setExtent())
        toArray: function() {
            return [this.northWest(), this.southEast()];
        }
    };

    MM.Extent.fromString = function(str) {
        var parts = str.split(/\s*,\s*/);
        if (parts.length != 4) {
            throw "Invalid extent string (expecting 4 comma-separated numbers)";
        }
        return new MM.Extent(
            parseFloat(parts[0]),
            parseFloat(parts[1]),
            parseFloat(parts[2]),
            parseFloat(parts[3])
        );
    };

    MM.Extent.fromArray = function(locations) {
        var extent = new MM.Extent();
        extent.setFromLocations(locations);
        return extent;
    };

    // Transformation
    // --------------
    MM.Transformation = function(ax, bx, cx, ay, by, cy) {
        this.ax = ax;
        this.bx = bx;
        this.cx = cx;
        this.ay = ay;
        this.by = by;
        this.cy = cy;
    };

    MM.Transformation.prototype = {

        ax: 0,
        bx: 0,
        cx: 0,
        ay: 0,
        by: 0,
        cy: 0,

        transform: function(point) {
            return new MM.Point(this.ax * point.x + this.bx * point.y + this.cx,
                                this.ay * point.x + this.by * point.y + this.cy);
        },

        untransform: function(point) {
            return new MM.Point((point.x * this.by - point.y * this.bx -
                               this.cx * this.by + this.cy * this.bx) /
                              (this.ax * this.by - this.ay * this.bx),
                              (point.x * this.ay - point.y * this.ax -
                               this.cx * this.ay + this.cy * this.ax) /
                              (this.bx * this.ay - this.by * this.ax));
        }

    };


    // Generates a transform based on three pairs of points,
    // a1 -> a2, b1 -> b2, c1 -> c2.
    MM.deriveTransformation = function(a1x, a1y, a2x, a2y,
                                       b1x, b1y, b2x, b2y,
                                       c1x, c1y, c2x, c2y) {
        var x = MM.linearSolution(a1x, a1y, a2x,
                                  b1x, b1y, b2x,
                                  c1x, c1y, c2x);
        var y = MM.linearSolution(a1x, a1y, a2y,
                                  b1x, b1y, b2y,
                                  c1x, c1y, c2y);
        return new MM.Transformation(x[0], x[1], x[2], y[0], y[1], y[2]);
    };

    // Solves a system of linear equations.
    //
    //     t1 = (a * r1) + (b + s1) + c
    //     t2 = (a * r2) + (b + s2) + c
    //     t3 = (a * r3) + (b + s3) + c
    //
    // r1 - t3 are the known values.
    // a, b, c are the unknowns to be solved.
    // returns the a, b, c coefficients.
    MM.linearSolution = function(r1, s1, t1, r2, s2, t2, r3, s3, t3) {
        // make them all floats
        r1 = parseFloat(r1);
        s1 = parseFloat(s1);
        t1 = parseFloat(t1);
        r2 = parseFloat(r2);
        s2 = parseFloat(s2);
        t2 = parseFloat(t2);
        r3 = parseFloat(r3);
        s3 = parseFloat(s3);
        t3 = parseFloat(t3);

        var a = (((t2 - t3) * (s1 - s2)) - ((t1 - t2) * (s2 - s3))) /
              (((r2 - r3) * (s1 - s2)) - ((r1 - r2) * (s2 - s3)));

        var b = (((t2 - t3) * (r1 - r2)) - ((t1 - t2) * (r2 - r3))) /
              (((s2 - s3) * (r1 - r2)) - ((s1 - s2) * (r2 - r3)));

        var c = t1 - (r1 * a) - (s1 * b);
        return [ a, b, c ];
    };
    // Projection
    // ----------

    // An abstract class / interface for projections
    MM.Projection = function(zoom, transformation) {
        if (!transformation) {
            transformation = new MM.Transformation(1, 0, 0, 0, 1, 0);
        }
        this.zoom = zoom;
        this.transformation = transformation;
    };

    MM.Projection.prototype = {

        zoom: 0,
        transformation: null,

        rawProject: function(point) {
            throw "Abstract method not implemented by subclass.";
        },

        rawUnproject: function(point) {
            throw "Abstract method not implemented by subclass.";
        },

        project: function(point) {
            point = this.rawProject(point);
            if(this.transformation) {
                point = this.transformation.transform(point);
            }
            return point;
        },

        unproject: function(point) {
            if(this.transformation) {
                point = this.transformation.untransform(point);
            }
            point = this.rawUnproject(point);
            return point;
        },

        locationCoordinate: function(location) {
            var point = new MM.Point(Math.PI * location.lon / 180.0,
                                     Math.PI * location.lat / 180.0);
            point = this.project(point);
            return new MM.Coordinate(point.y, point.x, this.zoom);
        },

        coordinateLocation: function(coordinate) {
            coordinate = coordinate.zoomTo(this.zoom);
            var point = new MM.Point(coordinate.column, coordinate.row);
            point = this.unproject(point);
            return new MM.Location(180.0 * point.y / Math.PI,
                                   180.0 * point.x / Math.PI);
        }
    };

    // A projection for equilateral maps, based on longitude and latitude
    MM.LinearProjection = function(zoom, transformation) {
        MM.Projection.call(this, zoom, transformation);
    };

    // The Linear projection doesn't reproject points
    MM.LinearProjection.prototype = {
        rawProject: function(point) {
            return new MM.Point(point.x, point.y);
        },
        rawUnproject: function(point) {
            return new MM.Point(point.x, point.y);
        }
    };

    MM.extend(MM.LinearProjection, MM.Projection);

    MM.MercatorProjection = function(zoom, transformation) {
        // super!
        MM.Projection.call(this, zoom, transformation);
    };

    // Project lon/lat points into meters required for Mercator
    MM.MercatorProjection.prototype = {
        rawProject: function(point) {
            return new MM.Point(point.x,
                         Math.log(Math.tan(0.25 * Math.PI + 0.5 * point.y)));
        },

        rawUnproject: function(point) {
            return new MM.Point(point.x,
                    2 * Math.atan(Math.pow(Math.E, point.y)) - 0.5 * Math.PI);
        }
    };

    MM.extend(MM.MercatorProjection, MM.Projection);
    // Providers
    // ---------
    // Providers provide tile URLs and possibly elements for layers.
    //
    // MapProvider ->
    //   Template
    //
    MM.MapProvider = function(getTile) {
        if (getTile) {
            this.getTile = getTile;
        }
    };

    MM.MapProvider.prototype = {

        // these are limits for available *tiles*
        // panning limits will be different (since you can wrap around columns)
        // but if you put Infinity in here it will screw up sourceCoordinate
        tileLimits: [
            new MM.Coordinate(0,0,0),             // top left outer
            new MM.Coordinate(1,1,0).zoomTo(18)   // bottom right inner
        ],

        getTileUrl: function(coordinate) {
            throw "Abstract method not implemented by subclass.";
        },

        getTile: function(coordinate) {
            throw "Abstract method not implemented by subclass.";
        },

        // releaseTile is not required
        releaseTile: function(element) { },

        // use this to tell MapProvider that tiles only exist between certain zoom levels.
        // should be set separately on Map to restrict interactive zoom/pan ranges
        setZoomRange: function(minZoom, maxZoom) {
            this.tileLimits[0] = this.tileLimits[0].zoomTo(minZoom);
            this.tileLimits[1] = this.tileLimits[1].zoomTo(maxZoom);
        },

        // wrap column around the world if necessary
        // return null if wrapped coordinate is outside of the tile limits
        sourceCoordinate: function(coord) {
            var TL = this.tileLimits[0].zoomTo(coord.zoom).container(),
                BR = this.tileLimits[1].zoomTo(coord.zoom),
                columnSize = Math.pow(2, coord.zoom),
                wrappedColumn;

            BR = new MM.Coordinate(Math.ceil(BR.row), Math.ceil(BR.column), Math.floor(BR.zoom));

            if (coord.column < 0) {
                wrappedColumn = ((coord.column % columnSize) + columnSize) % columnSize;
            } else {
                wrappedColumn = coord.column % columnSize;
            }

            if (coord.row < TL.row || coord.row >= BR.row) {
                return null;
            } else if (wrappedColumn < TL.column || wrappedColumn >= BR.column) {
                return null;
            } else {
                return new MM.Coordinate(coord.row, wrappedColumn, coord.zoom);
            }
        }
    };

    /**
     * FIXME: need a better explanation here! This is a pretty crucial part of
     * understanding how to use ModestMaps.
     *
     * TemplatedMapProvider is a tile provider that generates tile URLs from a
     * template string by replacing the following bits for each tile
     * coordinate:
     *
     * {Z}: the tile's zoom level (from 1 to ~20)
     * {X}: the tile's X, or column (from 0 to a very large number at higher
     * zooms)
     * {Y}: the tile's Y, or row (from 0 to a very large number at higher
     * zooms)
     *
     * E.g.:
     *
     * var osm = new MM.TemplatedMapProvider("http://tile.openstreetmap.org/{Z}/{X}/{Y}.png");
     *
     * Or:
     *
     * var placeholder = new MM.TemplatedMapProvider("http://placehold.it/256/f0f/fff.png&text={Z}/{X}/{Y}");
     *
     */
    MM.Template = function(template, subdomains) {
        var isQuadKey = template.match(/{(Q|quadkey)}/);
        // replace Microsoft style substitution strings
        if (isQuadKey) template = template
            .replace('{subdomain}', '{S}')
            .replace('{zoom}', '{Z}')
            .replace('{quadkey}', '{Q}');

        var hasSubdomains = (subdomains &&
            subdomains.length && template.indexOf("{S}") >= 0);

        function quadKey (row, column, zoom) {
            var key = '';
            for (var i = 1; i <= zoom; i++) {
                key += (((row >> zoom - i) & 1) << 1) | ((column >> zoom - i) & 1);
            }
            return key || '0';
        }

        var getTileUrl = function(coordinate) {
            var coord = this.sourceCoordinate(coordinate);
            if (!coord) {
                return null;
            }
            var base = template;
            if (hasSubdomains) {
                var index = parseInt(coord.zoom + coord.row + coord.column, 10) %
                    subdomains.length;
                base = base.replace('{S}', subdomains[index]);
            }
            if (isQuadKey) {
                return base
                    .replace('{Z}', coord.zoom.toFixed(0))
                    .replace('{Q}', quadKey(coord.row,
                        coord.column,
                        coord.zoom));
            } else {
                return base
                    .replace('{Z}', coord.zoom.toFixed(0))
                    .replace('{X}', coord.column.toFixed(0))
                    .replace('{Y}', coord.row.toFixed(0));
            }
        };

        MM.MapProvider.call(this, getTileUrl);
    };

    MM.Template.prototype = {
        // quadKey generator
        getTile: function(coord) {
          return this.getTileUrl(coord);
        }
    };

    MM.extend(MM.Template, MM.MapProvider);

    MM.TemplatedLayer = function(template, subdomains, name) {
      return new MM.Layer(new MM.Template(template, subdomains), null, name);
    };
    // Event Handlers
    // --------------

    // A utility function for finding the offset of the
    // mouse from the top-left of the page
    MM.getMousePoint = function(e, map) {
        // start with just the mouse (x, y)
        var point = new MM.Point(e.clientX, e.clientY);

        // correct for scrolled document
        point.x += document.body.scrollLeft + document.documentElement.scrollLeft;
        point.y += document.body.scrollTop + document.documentElement.scrollTop;

        // correct for nested offsets in DOM
        for (var node = map.parent; node; node = node.offsetParent) {
            point.x -= node.offsetLeft;
            point.y -= node.offsetTop;
        }
        return point;
    };

    MM.MouseWheelHandler = function() {
        var handler = { id: 'MouseWheelHandler' },
            map,
            _zoomDiv,
            prevTime,
            precise = false;


        function mouseWheel(e) {
            var delta = 0;
            prevTime = prevTime || new Date().getTime();

            try {
                _zoomDiv.scrollTop = 1000;
                _zoomDiv.dispatchEvent(e);
                delta = 1000 - _zoomDiv.scrollTop;
            } catch (error) {
                delta = e.wheelDelta || (-e.detail * 5);
            }

            // limit mousewheeling to once every 200ms
            var timeSince = new Date().getTime() - prevTime;
            var point = MM.getMousePoint(e, map);

            if (Math.abs(delta) > 0 && (timeSince > 200) && !precise) {
                map.zoomByAbout(delta > 0 ? 1 : -1, point);
                prevTime = new Date().getTime();
            } else if (precise) {
                map.zoomByAbout(delta * 0.001, point);
            }

            // Cancel the event so that the page doesn't scroll
            return MM.cancelEvent(e);
        }

        handler.init = function(x) {
            map = x;
            _zoomDiv = document.body.appendChild(document.createElement('div'));
            _zoomDiv.style.cssText = 'visibility:hidden;top:0;height:0;width:0;overflow-y:scroll';
            var innerDiv = _zoomDiv.appendChild(document.createElement('div'));
            innerDiv.style.height = '2000px';
            MM.addEvent(map.parent, 'mousewheel', mouseWheel);
            return handler;
        };

        handler.precise = function(x) {
            if (!arguments.length) return precise;
            precise = x;
            return handler;
        };

        handler.remove = function() {
            MM.removeEvent(map.parent, 'mousewheel', mouseWheel);
            _zoomDiv.parentNode.removeChild(_zoomDiv);
        };

        return handler;
    };

    MM.DoubleClickHandler = function() {
        var handler = { id: 'DoubleClickHandler' },
            map;

        function doubleClick(e) {
            // Ensure that this handler is attached once.
            // Get the point on the map that was double-clicked
            var point = MM.getMousePoint(e, map);
            // use shift-double-click to zoom out
            map.zoomByAbout(e.shiftKey ? -1 : 1, point);
            return MM.cancelEvent(e);
        }

        handler.init = function(x) {
            map = x;
            MM.addEvent(map.parent, 'dblclick', doubleClick);
            return handler;
        };

        handler.remove = function() {
            MM.removeEvent(map.parent, 'dblclick', doubleClick);
        };

        return handler;
    };

    // Handle the use of mouse dragging to pan the map.
    MM.DragHandler = function() {
        var handler = { id: 'DragHandler' },
            prevMouse,
            map;

        function mouseDown(e) {
            if (e.shiftKey || e.button == 2) return;
            MM.addEvent(document, 'mouseup', mouseUp);
            MM.addEvent(document, 'mousemove', mouseMove);

            prevMouse = new MM.Point(e.clientX, e.clientY);
            map.parent.style.cursor = 'move';

            return MM.cancelEvent(e);
        }

        function mouseUp(e) {
            MM.removeEvent(document, 'mouseup', mouseUp);
            MM.removeEvent(document, 'mousemove', mouseMove);

            prevMouse = null;
            map.parent.style.cursor = '';

            return MM.cancelEvent(e);
        }

        function mouseMove(e) {
            if (prevMouse) {
                map.panBy(
                    e.clientX - prevMouse.x,
                    e.clientY - prevMouse.y);
                prevMouse.x = e.clientX;
                prevMouse.y = e.clientY;
                prevMouse.t = +new Date();
            }

            return MM.cancelEvent(e);
        }

        handler.init = function(x) {
            map = x;
            MM.addEvent(map.parent, 'mousedown', mouseDown);
            return handler;
        };

        handler.remove = function() {
            MM.removeEvent(map.parent, 'mousedown', mouseDown);
        };

        return handler;
    };

    MM.MouseHandler = function() {
        var handler = { id: 'MouseHandler', handlers: [] },
            map;

        handler.init = function(x) {
            map = x;
            handler.handlers = [
                MM.DragHandler().init(map),
                MM.DoubleClickHandler().init(map),
                MM.MouseWheelHandler().init(map)
            ];
            return handler;
        };

        handler.remove = function() {
            for (var i = 0; i < handler.handlers.length; i++) {
                handler.handlers[i].remove();
            }
            handler.handlers = [];
            return handler;
        };

        return handler;
    };
    MM.TouchHandler = function() {
        var handler = { id: 'TouchHandler' },
            map,
            maxTapTime = 250,
            maxTapDistance = 30,
            maxDoubleTapDelay = 350,
            locations = {},
            taps = [],
            snapToZoom = true,
            wasPinching = false,
            lastPinchCenter = null;

        function isTouchable () {
          return 'ontouchstart' in window // works on most browsers 
              || 'onmsgesturechange' in window; // works on ie10
        }

        function updateTouches(e) {
            for (var i = 0; i < e.touches.length; i += 1) {
                var t = e.touches[i];
                if (t.identifier in locations) {
                    var l = locations[t.identifier];
                    l.x = t.clientX;
                    l.y = t.clientY;
                    l.scale = e.scale;
                }
                else {
                    locations[t.identifier] = {
                        scale: e.scale,
                        startPos: { x: t.clientX, y: t.clientY },
                        x: t.clientX,
                        y: t.clientY,
                        time: new Date().getTime()
                    };
                }
            }
        }

        // Test whether touches are from the same source -
        // whether this is the same touchmove event.
        function sameTouch (event, touch) {
            return (event && event.touch) &&
                (touch.identifier == event.touch.identifier);
        }

        function touchStart(e) {
            updateTouches(e);
        }

        function touchMove(e) {
            switch (e.touches.length) {
                case 1:
                    onPanning(e.touches[0]);
                    break;
                case 2:
                    onPinching(e);
                    break;
            }
            updateTouches(e);
            return MM.cancelEvent(e);
        }

        function touchEnd(e) {
            var now = new Date().getTime();
            // round zoom if we're done pinching
            if (e.touches.length === 0 && wasPinching) {
                onPinched(lastPinchCenter);
            }

            // Look at each changed touch in turn.
            for (var i = 0; i < e.changedTouches.length; i += 1) {
                var t = e.changedTouches[i],
                    loc = locations[t.identifier];
                // if we didn't see this one (bug?)
                // or if it was consumed by pinching already
                // just skip to the next one
                if (!loc || loc.wasPinch) {
                    continue;
                }

                // we now know we have an event object and a
                // matching touch that's just ended. Let's see
                // what kind of event it is based on how long it
                // lasted and how far it moved.
                var pos = { x: t.clientX, y: t.clientY },
                    time = now - loc.time,
                    travel = MM.Point.distance(pos, loc.startPos);
                if (travel > maxTapDistance) {
                    // we will to assume that the drag has been handled separately
                } else if (time > maxTapTime) {
                    // close in space, but not in time: a hold
                    pos.end = now;
                    pos.duration = time;
                    onHold(pos);
                } else {
                    // close in both time and space: a tap
                    pos.time = now;
                    onTap(pos);
                }
            }

            // Weird, sometimes an end event doesn't get thrown
            // for a touch that nevertheless has disappeared.
            // Still, this will eventually catch those ids:

            var validTouchIds = {};
            for (var j = 0; j < e.touches.length; j++) {
                validTouchIds[e.touches[j].identifier] = true;
            }
            for (var id in locations) {
                if (!(id in validTouchIds)) {
                    delete validTouchIds[id];
                }
            }

            return MM.cancelEvent(e);
        }

        function onHold (hold) {
            // TODO
        }

        // Handle a tap event - mainly watch for a doubleTap
        function onTap(tap) {
            if (taps.length &&
                (tap.time - taps[0].time) < maxDoubleTapDelay) {
                onDoubleTap(tap);
                taps = [];
                return;
            }
            taps = [tap];
        }

        // Handle a double tap by zooming in a single zoom level to a
        // round zoom.
        function onDoubleTap(tap) {
            var z = map.getZoom(), // current zoom
                tz = Math.round(z) + 1, // target zoom
                dz = tz - z;            // desired delate

            // zoom in to a round number
            var p = new MM.Point(tap.x, tap.y);
            map.zoomByAbout(dz, p);
        }

        // Re-transform the actual map parent's CSS transformation
        function onPanning (touch) {
            var pos = { x: touch.clientX, y: touch.clientY },
                prev = locations[touch.identifier];
            map.panBy(pos.x - prev.x, pos.y - prev.y);
        }

        function onPinching(e) {
            // use the first two touches and their previous positions
            var t0 = e.touches[0],
                t1 = e.touches[1],
                p0 = new MM.Point(t0.clientX, t0.clientY),
                p1 = new MM.Point(t1.clientX, t1.clientY),
                l0 = locations[t0.identifier],
                l1 = locations[t1.identifier];

            // mark these touches so they aren't used as taps/holds
            l0.wasPinch = true;
            l1.wasPinch = true;

            // scale about the center of these touches
            var center = MM.Point.interpolate(p0, p1, 0.5);

            map.zoomByAbout(
                Math.log(e.scale) / Math.LN2 -
                Math.log(l0.scale) / Math.LN2,
                center );

            // pan from the previous center of these touches
            var prevCenter = MM.Point.interpolate(l0, l1, 0.5);

            map.panBy(center.x - prevCenter.x,
                           center.y - prevCenter.y);
            wasPinching = true;
            lastPinchCenter = center;
        }

        // When a pinch event ends, round the zoom of the map.
        function onPinched(p) {
            // TODO: easing
            if (snapToZoom) {
                var z = map.getZoom(), // current zoom
                    tz =Math.round(z);     // target zoom
                map.zoomByAbout(tz - z, p);
            }
            wasPinching = false;
        }

        handler.init = function(x) {
            map = x;

            // Fail early if this isn't a touch device.
            if (!isTouchable()) return handler;

            MM.addEvent(map.parent, 'touchstart', touchStart);
            MM.addEvent(map.parent, 'touchmove', touchMove);
            MM.addEvent(map.parent, 'touchend', touchEnd);
            return handler;
        };

        handler.remove = function() {
            // Fail early if this isn't a touch device.
            if (!isTouchable()) return handler;

            MM.removeEvent(map.parent, 'touchstart', touchStart);
            MM.removeEvent(map.parent, 'touchmove', touchMove);
            MM.removeEvent(map.parent, 'touchend', touchEnd);
            return handler;
        };

        return handler;
    };
    // CallbackManager
    // ---------------
    // A general-purpose event binding manager used by `Map`
    // and `RequestManager`

    // Construct a new CallbackManager, with an list of
    // supported events.
    MM.CallbackManager = function(owner, events) {
        this.owner = owner;
        this.callbacks = {};
        for (var i = 0; i < events.length; i++) {
            this.callbacks[events[i]] = [];
        }
    };

    // CallbackManager does simple event management for modestmaps
    MM.CallbackManager.prototype = {
        // The element on which callbacks will be triggered.
        owner: null,

        // An object of callbacks in the form
        //
        //     { event: function }
        callbacks: null,

        // Add a callback to this object - where the `event` is a string of
        // the event name and `callback` is a function.
        addCallback: function(event, callback) {
            if (typeof(callback) == 'function' && this.callbacks[event]) {
                this.callbacks[event].push(callback);
            }
        },

        // Remove a callback. The given function needs to be equal (`===`) to
        // the callback added in `addCallback`, so named functions should be
        // used as callbacks.
        removeCallback: function(event, callback) {
            if (typeof(callback) == 'function' && this.callbacks[event]) {
                var cbs = this.callbacks[event],
                    len = cbs.length;
                for (var i = 0; i < len; i++) {
                  if (cbs[i] === callback) {
                    cbs.splice(i,1);
                    break;
                  }
                }
            }
        },

        // Trigger a callback, passing it an object or string from the second
        // argument.
        dispatchCallback: function(event, message) {
            if(this.callbacks[event]) {
                for (var i = 0; i < this.callbacks[event].length; i += 1) {
                    try {
                        this.callbacks[event][i](this.owner, message);
                    } catch(e) {
                        //console.log(e);
                        // meh
                    }
                }
            }
        }
    };
    // RequestManager
    // --------------
    // an image loading queue
    MM.RequestManager = function() {

        // The loading bay is a document fragment to optimize appending, since
        // the elements within are invisible. See
        //  [this blog post](http://ejohn.org/blog/dom-documentfragments/).
        this.loadingBay = document.createDocumentFragment();

        this.requestsById = {};
        this.openRequestCount = 0;

        this.maxOpenRequests = 4;
        this.requestQueue = [];

        this.callbackManager = new MM.CallbackManager(this, [
            'requestcomplete', 'requesterror']);
    };

    MM.RequestManager.prototype = {

        // DOM element, hidden, for making sure images dispatch complete events
        loadingBay: null,

        // all known requests, by ID
        requestsById: null,

        // current pending requests
        requestQueue: null,

        // current open requests (children of loadingBay)
        openRequestCount: null,

        // the number of open requests permitted at one time, clamped down
        // because of domain-connection limits.
        maxOpenRequests: null,

        // for dispatching 'requestcomplete'
        callbackManager: null,

        addCallback: function(event, callback) {
            this.callbackManager.addCallback(event,callback);
        },

        removeCallback: function(event, callback) {
            this.callbackManager.removeCallback(event,callback);
        },

        dispatchCallback: function(event, message) {
            this.callbackManager.dispatchCallback(event,message);
        },

        // Clear everything in the queue by excluding nothing
        clear: function() {
            this.clearExcept({});
        },

        clearRequest: function(id) {
            if(id in this.requestsById) {
                delete this.requestsById[id];
            }

            for(var i = 0; i < this.requestQueue.length; i++) {
                var request = this.requestQueue[i];
                if(request && request.id == id) {
                    this.requestQueue[i] = null;
                }
            }
        },

        // Clear everything in the queue except for certain keys, specified
        // by an object of the form
        //
        //     { key: throwawayvalue }
        clearExcept: function(validIds) {

            // clear things from the queue first...
            for (var i = 0; i < this.requestQueue.length; i++) {
                var request = this.requestQueue[i];
                if (request && !(request.id in validIds)) {
                    this.requestQueue[i] = null;
                }
            }

            // then check the loadingBay...
            var openRequests = this.loadingBay.childNodes;
            for (var j = openRequests.length-1; j >= 0; j--) {
                var img = openRequests[j];
                if (!(img.id in validIds)) {
                    this.loadingBay.removeChild(img);
                    this.openRequestCount--;
                    /* console.log(this.openRequestCount + " open requests"); */
                    img.src = img.coord = img.onload = img.onerror = null;
                }
            }

            // hasOwnProperty protects against prototype additions
            // > "The standard describes an augmentable Object.prototype.
            //  Ignore standards at your own peril."
            // -- http://www.yuiblog.com/blog/2006/09/26/for-in-intrigue/
            for (var id in this.requestsById) {
                if (!(id in validIds)) {
                    if (this.requestsById.hasOwnProperty(id)) {
                        var requestToRemove = this.requestsById[id];
                        // whether we've done the request or not...
                        delete this.requestsById[id];
                        if (requestToRemove !== null) {
                            requestToRemove =
                                requestToRemove.id =
                                requestToRemove.coord =
                                requestToRemove.url = null;
                        }
                    }
                }
            }
        },

        // Given a tile id, check whether the RequestManager is currently
        // requesting it and waiting for the result.
        hasRequest: function(id) {
            return (id in this.requestsById);
        },

        // * TODO: remove dependency on coord (it's for sorting, maybe call it data?)
        // * TODO: rename to requestImage once it's not tile specific
        requestTile: function(id, coord, url) {
            if (!(id in this.requestsById)) {
                var request = { id: id, coord: coord.copy(), url: url };
                // if there's no url just make sure we don't request this image again
                this.requestsById[id] = request;
                if (url) {
                    this.requestQueue.push(request);
                    /* console.log(this.requestQueue.length + ' pending requests'); */
                }
            }
        },

        getProcessQueue: function() {
            // let's only create this closure once...
            if (!this._processQueue) {
                var theManager = this;
                this._processQueue = function() {
                    theManager.processQueue();
                };
            }
            return this._processQueue;
        },

        // Select images from the `requestQueue` and create image elements for
        // them, attaching their load events to the function returned by
        // `this.getLoadComplete()` so that they can be added to the map.
        processQueue: function(sortFunc) {
            // When the request queue fills up beyond 8, start sorting the
            // requests so that spiral-loading or another pattern can be used.
            if (sortFunc && this.requestQueue.length > 8) {
                this.requestQueue.sort(sortFunc);
            }
            while (this.openRequestCount < this.maxOpenRequests && this.requestQueue.length > 0) {
                var request = this.requestQueue.pop();
                if (request) {
                    this.openRequestCount++;
                    /* console.log(this.openRequestCount + ' open requests'); */

                    // JSLitmus benchmark shows createElement is a little faster than
                    // new Image() in Firefox and roughly the same in Safari:
                    // http://tinyurl.com/y9wz2jj http://tinyurl.com/yes6rrt
                    var img = document.createElement('img');

                    // FIXME: id is technically not unique in document if there
                    // are two Maps but toKey is supposed to be fast so we're trying
                    // to avoid a prefix ... hence we can't use any calls to
                    // `document.getElementById()` to retrieve images
                    img.id = request.id;
                    img.style.position = 'absolute';
                    // * FIXME: store this elsewhere to avoid scary memory leaks?
                    // * FIXME: call this 'data' not 'coord' so that RequestManager is less Tile-centric?
                    img.coord = request.coord;
                    // add it to the DOM in a hidden layer, this is a bit of a hack, but it's
                    // so that the event we get in image.onload has srcElement assigned in IE6
                    this.loadingBay.appendChild(img);
                    // set these before img.src to avoid missing an img that's already cached
                    img.onload = img.onerror = this.getLoadComplete();
                    img.src = request.url;

                    // keep things tidy
                    request = request.id = request.coord = request.url = null;
                }
            }
        },

        _loadComplete: null,

        // Get the singleton `_loadComplete` function that is called on image
        // load events, either removing them from the queue and dispatching an
        // event to add them to the map, or deleting them if the image failed
        // to load.
        getLoadComplete: function() {
            // let's only create this closure once...
            if (!this._loadComplete) {
                var theManager = this;
                this._loadComplete = function(e) {
                    // this is needed because we don't use MM.addEvent for images
                    e = e || window.event;

                    // srcElement for IE, target for FF, Safari etc.
                    var img = e.srcElement || e.target;

                    // unset these straight away so we don't call this twice
                    img.onload = img.onerror = null;

                    // pull it back out of the (hidden) DOM
                    // so that draw will add it correctly later
                    theManager.loadingBay.removeChild(img);
                    theManager.openRequestCount--;
                    delete theManager.requestsById[img.id];

                    /* console.log(theManager.openRequestCount + ' open requests'); */

                    // NB:- complete is also true onerror if we got a 404
                    if (e.type === 'load' && (img.complete ||
                        (img.readyState && img.readyState == 'complete'))) {
                        theManager.dispatchCallback('requestcomplete', img);
                    } else {
                        // if it didn't finish clear its src to make sure it
                        // really stops loading
                        // FIXME: we'll never retry because this id is still
                        // in requestsById - is that right?
                        theManager.dispatchCallback('requesterror', {
                            element: img,
                            url: ('' + img.src)
                        });
                        img.src = null;
                    }

                    // keep going in the same order
                    // use `setTimeout()` to avoid the IE recursion limit, see
                    // http://cappuccino.org/discuss/2010/03/01/internet-explorer-global-variables-and-stack-overflows/
                    // and https://github.com/stamen/modestmaps-js/issues/12
                    setTimeout(theManager.getProcessQueue(), 0);

                };
            }
            return this._loadComplete;
        }

    };
    // Layer
    MM.Layer = function(provider, parent, name) {
        this.parent = parent || document.createElement('div');
        this.parent.style.cssText = 'position: absolute; top: 0px; left: 0px; width: 100%; height: 100%; margin: 0; padding: 0; z-index: 0';
        this.name = name;
        this.levels = {};
        this.requestManager = new MM.RequestManager();
        this.requestManager.addCallback('requestcomplete', this.getTileComplete());
        this.requestManager.addCallback('requesterror', this.getTileError());
        if (provider) this.setProvider(provider);
    };

    MM.Layer.prototype = {

        map: null, // TODO: remove
        parent: null,
        name: null,
        enabled: true,
        tiles: null,
        levels: null,
        requestManager: null,
        provider: null,
        _tileComplete: null,

        getTileComplete: function() {
            if (!this._tileComplete) {
                var theLayer = this;
                this._tileComplete = function(manager, tile) {
                    theLayer.tiles[tile.id] = tile;
                    theLayer.positionTile(tile);
                    // Support style transition if available.
                    tile.style.visibility = 'inherit';
                    tile.className = 'map-tile-loaded';
                };
            }
            return this._tileComplete;
        },

        getTileError: function() {
            if (!this._tileError) {
                var theLayer = this;
                this._tileError = function(manager, tile) {
                    tile.onload = tile.onerror = null;
                    theLayer.tiles[tile.element.id] = tile.element;
                    theLayer.positionTile(tile.element);
                    tile.element.style.visibility = 'hidden';
                };
            }
            return this._tileError;
        },

        draw: function() {
            if (!this.enabled || !this.map) return;
            // compares manhattan distance from center of
            // requested tiles to current map center
            // NB:- requested tiles are *popped* from queue, so we do a descending sort
            var theCoord = this.map.coordinate.zoomTo(Math.round(this.map.coordinate.zoom));

            function centerDistanceCompare(r1, r2) {
                if (r1 && r2) {
                    var c1 = r1.coord;
                    var c2 = r2.coord;
                    if (c1.zoom == c2.zoom) {
                        var ds1 = Math.abs(theCoord.row - c1.row - 0.5) +
                                  Math.abs(theCoord.column - c1.column - 0.5);
                        var ds2 = Math.abs(theCoord.row - c2.row - 0.5) +
                                  Math.abs(theCoord.column - c2.column - 0.5);
                        return ds1 < ds2 ? 1 : ds1 > ds2 ? -1 : 0;
                    } else {
                        return c1.zoom < c2.zoom ? 1 : c1.zoom > c2.zoom ? -1 : 0;
                    }
                }
                return r1 ? 1 : r2 ? -1 : 0;
            }

            // if we're in between zoom levels, we need to choose the nearest:
            var baseZoom = Math.round(this.map.coordinate.zoom);

            // these are the top left and bottom right tile coordinates
            // we'll be loading everything in between:
            var startCoord = this.map.pointCoordinate(new MM.Point(0,0))
                .zoomTo(baseZoom).container();
            var endCoord = this.map.pointCoordinate(this.map.dimensions)
                .zoomTo(baseZoom).container().right().down();

            // tiles with invalid keys will be removed from visible levels
            // requests for tiles with invalid keys will be canceled
            // (this object maps from a tile key to a boolean)
            var validTileKeys = { };

            // make sure we have a container for tiles in the current level
            var levelElement = this.createOrGetLevel(startCoord.zoom);

            // use this coordinate for generating keys, parents and children:
            var tileCoord = startCoord.copy();

            for (tileCoord.column = startCoord.column;
                 tileCoord.column < endCoord.column; tileCoord.column++) {
                for (tileCoord.row = startCoord.row;
                     tileCoord.row < endCoord.row; tileCoord.row++) {
                    var validKeys = this.inventoryVisibleTile(levelElement, tileCoord);

                    while (validKeys.length) {
                        validTileKeys[validKeys.pop()] = true;
                    }
                }
            }

            // i from i to zoom-5 are levels that would be scaled too big,
            // i from zoom + 2 to levels. length are levels that would be
            // scaled too small (and tiles would be too numerous)
            for (var name in this.levels) {
                if (this.levels.hasOwnProperty(name)) {
                    var zoom = parseInt(name,10);

                    if (zoom >= startCoord.zoom - 5 && zoom < startCoord.zoom + 2) {
                        continue;
                    }

                    var level = this.levels[name];
                    level.style.display = 'none';
                    var visibleTiles = this.tileElementsInLevel(level);

                    while (visibleTiles.length) {
                        this.provider.releaseTile(visibleTiles[0].coord);
                        this.requestManager.clearRequest(visibleTiles[0].coord.toKey());
                        level.removeChild(visibleTiles[0]);
                        visibleTiles.shift();
                    }
                }
            }

            // levels we want to see, if they have tiles in validTileKeys
            var minLevel = startCoord.zoom - 5;
            var maxLevel = startCoord.zoom + 2;

            for (var z = minLevel; z < maxLevel; z++) {
                this.adjustVisibleLevel(this.levels[z], z, validTileKeys);
            }

            // cancel requests that aren't visible:
            this.requestManager.clearExcept(validTileKeys);

            // get newly requested tiles, sort according to current view:
            this.requestManager.processQueue(centerDistanceCompare);
        },

        // For a given tile coordinate in a given level element, ensure that it's
        // correctly represented in the DOM including potentially-overlapping
        // parent and child tiles for pyramid loading.
        //
        // Return a list of valid (i.e. loadable?) tile keys.
        inventoryVisibleTile: function(layer_element, tile_coord) {
            var tile_key = tile_coord.toKey(),
                valid_tile_keys = [tile_key];

            // Check that the needed tile already exists someplace - add it to the DOM if it does.
            if (tile_key in this.tiles) {
                var tile = this.tiles[tile_key];

                // ensure it's in the DOM:
                if (tile.parentNode != layer_element) {
                    layer_element.appendChild(tile);
                    // if the provider implements reAddTile(), call it
                    if ("reAddTile" in this.provider) {
                        this.provider.reAddTile(tile_key, tile_coord, tile);
                    }
                }

                return valid_tile_keys;
            }

            // Check that the needed tile has even been requested at all.
            if (!this.requestManager.hasRequest(tile_key)) {
                var tileToRequest = this.provider.getTile(tile_coord);
                if (typeof tileToRequest == 'string') {
                    this.addTileImage(tile_key, tile_coord, tileToRequest);
                // tile must be truish
                } else if (tileToRequest) {
                    this.addTileElement(tile_key, tile_coord, tileToRequest);
                }
            }

            // look for a parent tile in our image cache
            var tileCovered = false;
            var maxStepsOut = tile_coord.zoom;

            for (var pz = 1; pz <= maxStepsOut; pz++) {
                var parent_coord = tile_coord.zoomBy(-pz).container();
                var parent_key = parent_coord.toKey();

                // only mark it valid if we have it already
                if (parent_key in this.tiles) {
                    valid_tile_keys.push(parent_key);
                    tileCovered = true;
                    break;
                }
            }

            // if we didn't find a parent, look at the children:
            if (!tileCovered) {
                var child_coord = tile_coord.zoomBy(1);

                // mark everything valid whether or not we have it:
                valid_tile_keys.push(child_coord.toKey());
                child_coord.column += 1;
                valid_tile_keys.push(child_coord.toKey());
                child_coord.row += 1;
                valid_tile_keys.push(child_coord.toKey());
                child_coord.column -= 1;
                valid_tile_keys.push(child_coord.toKey());
            }

            return valid_tile_keys;
        },

        tileElementsInLevel: function(level) {
            // this is somewhat future proof, we're looking for DOM elements
            // not necessarily <img> elements
            var tiles = [];
            for (var tile = level.firstChild; tile; tile = tile.nextSibling) {
                if (tile.nodeType == 1) {
                    tiles.push(tile);
                }
            }
            return tiles;
        },

        /**
         * For a given level, adjust visibility as a whole and discard individual
         * tiles based on values in valid_tile_keys from inventoryVisibleTile().
         */
        adjustVisibleLevel: function(level, zoom, valid_tile_keys) {
            // no tiles for this level yet
            if (!level) return;

            var scale = 1;
            var theCoord = this.map.coordinate.copy();

            if (level.childNodes.length > 0) {
                level.style.display = 'block';
                scale = Math.pow(2, this.map.coordinate.zoom - zoom);
                theCoord = theCoord.zoomTo(zoom);
            } else {
                level.style.display = 'none';
                return false;
            }

            var tileWidth = this.map.tileSize.x * scale;
            var tileHeight = this.map.tileSize.y * scale;
            var center = new MM.Point(this.map.dimensions.x* 0.5, this.map.dimensions.y* 0.5);
            var tiles = this.tileElementsInLevel(level);

            while (tiles.length) {
                var tile = tiles.pop();

                if (!valid_tile_keys[tile.id]) {
                    this.provider.releaseTile(tile.coord);
                    this.requestManager.clearRequest(tile.coord.toKey());
                    level.removeChild(tile);
                } else {
                    // position tiles
                    MM.moveElement(tile, {
                        x: Math.round(center.x +
                            (tile.coord.column - theCoord.column) * tileWidth),
                        y: Math.round(center.y +
                            (tile.coord.row - theCoord.row) * tileHeight),
                        scale: scale,
                        // TODO: pass only scale or only w/h
                        width: this.map.tileSize.x,
                        height: this.map.tileSize.y
                    });
                }
            }
        },

        createOrGetLevel: function(zoom) {
            if (zoom in this.levels) {
                return this.levels[zoom];
            }

            var level = document.createElement('div');
            level.id = this.parent.id + '-zoom-' + zoom;
            level.style.cssText = 'position: absolute; top: 0px; left: 0px; width: 100%; height: 100%; margin: 0; padding: 0;';
            level.style.zIndex = zoom;

            this.parent.appendChild(level);
            this.levels[zoom] = level;

            return level;
        },

        addTileImage: function(key, coord, url) {
            this.requestManager.requestTile(key, coord, url);
        },

        addTileElement: function(key, coordinate, element) {
            // Expected in draw()
            element.id = key;
            element.coord = coordinate.copy();
            this.positionTile(element);
        },

        positionTile: function(tile) {
            // position this tile (avoids a full draw() call):
            var theCoord = this.map.coordinate.zoomTo(tile.coord.zoom);

            // Start tile positioning and prevent drag for modern browsers
            tile.style.cssText = 'position:absolute;-webkit-user-select:none;' +
                '-webkit-user-drag:none;-moz-user-drag:none;-webkit-transform-origin:0 0;' +
                '-moz-transform-origin:0 0;-o-transform-origin:0 0;-ms-transform-origin:0 0;' +
                'width:' + this.map.tileSize.x + 'px; height: ' + this.map.tileSize.y + 'px;';

            // Prevent drag for IE
            tile.ondragstart = function() { return false; };

            var scale = Math.pow(2, this.map.coordinate.zoom - tile.coord.zoom);

            MM.moveElement(tile, {
                x: Math.round((this.map.dimensions.x* 0.5) +
                    (tile.coord.column - theCoord.column) * this.map.tileSize.x * scale),
                y: Math.round((this.map.dimensions.y* 0.5) +
                    (tile.coord.row - theCoord.row) * this.map.tileSize.y * scale),
                scale: scale,
                // TODO: pass only scale or only w/h
                width: this.map.tileSize.x,
                height: this.map.tileSize.y
            });

            // add tile to its level
            var theLevel = this.levels[tile.coord.zoom];
            theLevel.appendChild(tile);


            // ensure the level is visible if it's still the current level
            if (Math.round(this.map.coordinate.zoom) == tile.coord.zoom) {
                theLevel.style.display = 'block';
            }

            // request a lazy redraw of all levels
            // this will remove tiles that were only visible
            // to cover this tile while it loaded:
            this.requestRedraw();
        },

        _redrawTimer: undefined,

        requestRedraw: function() {
            // we'll always draw within 1 second of this request,
            // sometimes faster if there's already a pending redraw
            // this is used when a new tile arrives so that we clear
            // any parent/child tiles that were only being displayed
            // until the tile loads at the right zoom level
            if (!this._redrawTimer) {
                this._redrawTimer = setTimeout(this.getRedraw(), 1000);
            }
        },

        _redraw: null,

        getRedraw: function() {
            // let's only create this closure once...
            if (!this._redraw) {
                var theLayer = this;
                this._redraw = function() {
                    theLayer.draw();
                    theLayer._redrawTimer = 0;
                };
            }
            return this._redraw;
        },

        setProvider: function(newProvider) {
            var firstProvider = (this.provider === null);

            // if we already have a provider the we'll need to
            // clear the DOM, cancel requests and redraw
            if (!firstProvider) {
                this.requestManager.clear();

                for (var name in this.levels) {
                    if (this.levels.hasOwnProperty(name)) {
                        var level = this.levels[name];

                        while (level.firstChild) {
                            this.provider.releaseTile(level.firstChild.coord);
                            level.removeChild(level.firstChild);
                        }
                    }
                }
            }

            // first provider or not we'll init/reset some values...
            this.tiles = {};

            // for later: check geometry of old provider and set a new coordinate center
            // if needed (now? or when?)
            this.provider = newProvider;

            if (!firstProvider) {
                this.draw();
            }
        },

        // Enable a layer and show its dom element
        enable: function() {
            this.enabled = true;
            this.parent.style.display = '';
            this.draw();
            return this;
        },

        // Disable a layer, don't display in DOM, clear all requests
        disable: function() {
            this.enabled = false;
            this.requestManager.clear();
            this.parent.style.display = 'none';
            return this;
        },

        // Remove this layer from the DOM, cancel all of its requests
        // and unbind any callbacks that are bound to it.
        destroy: function() {
            this.requestManager.clear();
            this.requestManager.removeCallback('requestcomplete', this.getTileComplete());
            this.requestManager.removeCallback('requesterror', this.getTileError());
            // TODO: does requestManager need a destroy function too?
            this.provider = null;
            // If this layer was ever attached to the DOM, detach it.
            if (this.parent.parentNode) {
              this.parent.parentNode.removeChild(this.parent);
            }
            this.map = null;
        }
    };

    // Map

    // Instance of a map intended for drawing to a div.
    //
    //  * `parent` (required DOM element)
    //      Can also be an ID of a DOM element
    //  * `layerOrLayers` (required MM.Layer or Array of MM.Layers)
    //      each one must implement draw(), destroy(), have a .parent DOM element and a .map property
    //      (an array of URL templates or MM.MapProviders is also acceptable)
    //  * `dimensions` (optional Point)
    //      Size of map to create
    //  * `eventHandlers` (optional Array)
    //      If empty or null MouseHandler will be used
    //      Otherwise, each handler will be called with init(map)
    MM.Map = function(parent, layerOrLayers, dimensions, eventHandlers) {

        if (typeof parent == 'string') {
            parent = document.getElementById(parent);
            if (!parent) {
                throw 'The ID provided to modest maps could not be found.';
            }
        }
        this.parent = parent;

        // we're no longer adding width and height to parent.style but we still
        // need to enforce padding, overflow and position otherwise everything screws up
        // TODO: maybe console.warn if the current values are bad?
        this.parent.style.padding = '0';
        this.parent.style.overflow = 'hidden';

        var position = MM.getStyle(this.parent, 'position');
        if (position != 'relative' && position != 'absolute') {
            this.parent.style.position = 'relative';
        }

        this.layers = [];

        if (!layerOrLayers) {
            layerOrLayers = [];
        }

        if (!(layerOrLayers instanceof Array)) {
            layerOrLayers = [ layerOrLayers ];
        }

        for (var i = 0; i < layerOrLayers.length; i++) {
            this.addLayer(layerOrLayers[i]);
        }

        // default to Google-y Mercator style maps
        this.projection = new MM.MercatorProjection(0,
            MM.deriveTransformation(-Math.PI,  Math.PI, 0, 0,
                                     Math.PI,  Math.PI, 1, 0,
                                    -Math.PI, -Math.PI, 0, 1));
        this.tileSize = new MM.Point(256, 256);

        // default 0-18 zoom level
        // with infinite horizontal pan and clamped vertical pan
        this.coordLimits = [
            new MM.Coordinate(0,-Infinity,0),           // top left outer
            new MM.Coordinate(1,Infinity,0).zoomTo(18) // bottom right inner
        ];

        // eyes towards null island
        this.coordinate = new MM.Coordinate(0.5, 0.5, 0);

        // if you don't specify dimensions we assume you want to fill the parent
        // unless the parent has no w/h, in which case we'll still use a default
        if (!dimensions) {
            dimensions = new MM.Point(this.parent.offsetWidth,
                                      this.parent.offsetHeight);
            this.autoSize = true;
            // use destroy to get rid of this handler from the DOM
            MM.addEvent(window, 'resize', this.windowResize());
        } else {
            this.autoSize = false;
            // don't call setSize here because it calls draw()
            this.parent.style.width = Math.round(dimensions.x) + 'px';
            this.parent.style.height = Math.round(dimensions.y) + 'px';
        }
        this.dimensions = dimensions;

        this.callbackManager = new MM.CallbackManager(this, [
            'zoomed',
            'panned',
            'centered',
            'extentset',
            'resized',
            'drawn'
        ]);

        // set up handlers last so that all required attributes/functions are in place if needed
        if (eventHandlers === undefined) {
            this.eventHandlers = [
                MM.MouseHandler().init(this),
                MM.TouchHandler().init(this)
            ];
        } else {
            this.eventHandlers = eventHandlers;
            if (eventHandlers instanceof Array) {
                for (var j = 0; j < eventHandlers.length; j++) {
                    eventHandlers[j].init(this);
                }
            }
        }

    };

    MM.Map.prototype = {

        parent: null,          // DOM Element
        dimensions: null,      // MM.Point with x/y size of parent element

        projection: null,      // MM.Projection of first known layer
        coordinate: null,      // Center of map MM.Coordinate with row/column/zoom
        tileSize: null,        // MM.Point with x/y size of tiles

        coordLimits: null,     // Array of [ topLeftOuter, bottomLeftInner ] MM.Coordinates

        layers: null,          // Array of MM.Layer (interface = .draw(), .destroy(), .parent and .map)

        callbackManager: null, // MM.CallbackManager, handles map events

        eventHandlers: null,   // Array of interaction handlers, just a MM.MouseHandler by default

        autoSize: null,        // Boolean, true if we have a window resize listener

        toString: function() {
            return 'Map(#' + this.parent.id + ')';
        },

        // callbacks...
        addCallback: function(event, callback) {
            this.callbackManager.addCallback(event, callback);
            return this;
        },

        removeCallback: function(event, callback) {
            this.callbackManager.removeCallback(event, callback);
            return this;
        },

        dispatchCallback: function(event, message) {
            this.callbackManager.dispatchCallback(event, message);
            return this;
        },

        // event handlers
        getHandler: function (id, mHandler) {
            var handler = null,
                rel = this.eventHandlers;

            if (mHandler === true) {
                rel = this.getHandler('MouseHandler', false);
                if (!rel) return handler;
                rel = rel.handlers;
            }

            if (!rel) return handler;

            for (var l = 0; l < rel.length; l++) {
                if (typeof rel[l].id === 'string' && rel[l].id === id) {
                    handler = rel[l];
                    break;
                }
            }

            // When no handler was found, attempt to find it in MouseHandler's handlers
            if (!handler && typeof mHandler !== 'boolean') {
                return this.getHandler(id, true);
            }

            return handler;
        },

        disableHandler: function (id) {
            var handler = this.getHandler(id);
            if (handler && typeof handler.remove === 'function') handler.remove();
            return this;
        },

        enableHandler: function (id) {
            var handler = this.getHandler(id);
            if (handler && typeof handler.remove === 'function') handler.init(this);
            return this;
        },

        removeHandler: function(id) {
            var handler = this.getHandler(id),
                index = this.eventHandlers.indexOf(handler);

            // When no handler was found, try to find it in MouseHandler's handlers
            if (index === -1) {
                var mouseHandler = this.getHandler('MouseHandler');
                index = mouseHandler.handlers.indexOf(handler);
                if (index !== -1) {
                    handler.remove();
                    mouseHandler.handlers.splice(index,1);
                }
                return;
            }

            handler.remove();
            this.eventHandlers.splice(index,1);
            return handler;
        },

        addHandler: function (handler) {
            handler.init(this);
            if (typeof handler.id !== 'string') {
                throw new Error('Handler lacks the required id attribute');
            }
            this.eventHandlers.push(handler);
            return this;
        },

        windowResize: function() {
            if (!this._windowResize) {
                var theMap = this;
                this._windowResize = function(event) {
                    // don't call setSize here because it sets parent.style.width/height
                    // and setting the height breaks percentages and default styles
                    theMap.dimensions = new MM.Point(theMap.parent.offsetWidth, theMap.parent.offsetHeight);
                    theMap.draw();
                    theMap.dispatchCallback('resized', [theMap.dimensions]);
                };
            }
            return this._windowResize;
        },

        // A convenience function to restrict interactive zoom ranges.
        // (you should also adjust map provider to restrict which tiles get loaded,
        // or modify map.coordLimits and provider.tileLimits for finer control)
        setZoomRange: function(minZoom, maxZoom) {
            this.coordLimits[0] = this.coordLimits[0].zoomTo(minZoom);
            this.coordLimits[1] = this.coordLimits[1].zoomTo(maxZoom);
            return this;
        },

        // zooming
        zoomBy: function(zoomOffset) {
            this.coordinate = this.enforceLimits(this.coordinate.zoomBy(zoomOffset));
            MM.getFrame(this.getRedraw());
            this.dispatchCallback('zoomed', zoomOffset);
            return this;
        },

        zoomIn: function()  { return this.zoomBy(1); },
        zoomOut: function()  { return this.zoomBy(-1); },
        setZoom: function(z) { return this.zoomBy(z - this.coordinate.zoom); },

        zoomByAbout: function(zoomOffset, point) {
            var location = this.pointLocation(point);

            this.coordinate = this.enforceLimits(this.coordinate.zoomBy(zoomOffset));
            var newPoint = this.locationPoint(location);

            this.dispatchCallback('zoomed', zoomOffset);
            return this.panBy(point.x - newPoint.x, point.y - newPoint.y);
        },

        // panning
        panBy: function(dx, dy) {
            this.coordinate.column -= dx / this.tileSize.x;
            this.coordinate.row -= dy / this.tileSize.y;

            this.coordinate = this.enforceLimits(this.coordinate);

            // Defer until the browser is ready to draw.
            MM.getFrame(this.getRedraw());
            this.dispatchCallback('panned', [dx, dy]);
            return this;
        },

        panLeft: function() { return this.panBy(100, 0); },
        panRight: function() { return this.panBy(-100, 0); },
        panDown: function() { return this.panBy(0, -100); },
        panUp: function() { return this.panBy(0, 100); },

        // positioning
        setCenter: function(location) {
            return this.setCenterZoom(location, this.coordinate.zoom);
        },

        setCenterZoom: function(location, zoom) {
            this.coordinate = this.projection.locationCoordinate(location).zoomTo(parseFloat(zoom) || 0);
            this.coordinate = this.enforceLimits(this.coordinate);
            MM.getFrame(this.getRedraw());
            this.dispatchCallback('centered', [location, zoom]);
            return this;
        },

        extentCoordinate: function(locations, precise) {
            // coerce locations to an array if it's a Extent instance
            if (locations instanceof MM.Extent) {
                locations = locations.toArray();
            }

            var TL, BR;
            for (var i = 0; i < locations.length; i++) {
                var coordinate = this.projection.locationCoordinate(locations[i]);
                if (TL) {
                    TL.row = Math.min(TL.row, coordinate.row);
                    TL.column = Math.min(TL.column, coordinate.column);
                    TL.zoom = Math.min(TL.zoom, coordinate.zoom);
                    BR.row = Math.max(BR.row, coordinate.row);
                    BR.column = Math.max(BR.column, coordinate.column);
                    BR.zoom = Math.max(BR.zoom, coordinate.zoom);
                }
                else {
                    TL = coordinate.copy();
                    BR = coordinate.copy();
                }
            }

            var width = this.dimensions.x + 1;
            var height = this.dimensions.y + 1;

            // multiplication factor between horizontal span and map width
            var hFactor = (BR.column - TL.column) / (width / this.tileSize.x);

            // multiplication factor expressed as base-2 logarithm, for zoom difference
            var hZoomDiff = Math.log(hFactor) / Math.log(2);

            // possible horizontal zoom to fit geographical extent in map width
            var hPossibleZoom = TL.zoom - (precise ? hZoomDiff : Math.ceil(hZoomDiff));

            // multiplication factor between vertical span and map height
            var vFactor = (BR.row - TL.row) / (height / this.tileSize.y);

            // multiplication factor expressed as base-2 logarithm, for zoom difference
            var vZoomDiff = Math.log(vFactor) / Math.log(2);

            // possible vertical zoom to fit geographical extent in map height
            var vPossibleZoom = TL.zoom - (precise ? vZoomDiff : Math.ceil(vZoomDiff));

            // initial zoom to fit extent vertically and horizontally
            var initZoom = Math.min(hPossibleZoom, vPossibleZoom);

            // additionally, make sure it's not outside the boundaries set by map limits
            initZoom = Math.min(initZoom, this.coordLimits[1].zoom);
            initZoom = Math.max(initZoom, this.coordLimits[0].zoom);

            // coordinate of extent center
            var centerRow = (TL.row + BR.row) / 2;
            var centerColumn = (TL.column + BR.column) / 2;
            var centerZoom = TL.zoom;
            return new MM.Coordinate(centerRow, centerColumn, centerZoom).zoomTo(initZoom);
        },

        setExtent: function(locations, precise) {
            this.coordinate = this.extentCoordinate(locations, precise);
            this.coordinate = this.enforceLimits(this.coordinate);
            MM.getFrame(this.getRedraw());

            this.dispatchCallback('extentset', locations);
            return this;
        },

        // Resize the map's container `<div>`, redrawing the map and triggering
        // `resized` to make sure that the map's presentation is still correct.
        setSize: function(dimensions) {
            // Ensure that, whether a raw object or a Point object is passed,
            // this.dimensions will be a Point.
            this.dimensions = new MM.Point(dimensions.x, dimensions.y);
            this.parent.style.width = Math.round(this.dimensions.x) + 'px';
            this.parent.style.height = Math.round(this.dimensions.y) + 'px';
            if (this.autoSize) {
                MM.removeEvent(window, 'resize', this.windowResize());
                this.autoSize = false;
            }
            this.draw(); // draw calls enforceLimits
            // (if you switch to getFrame, call enforceLimits first)
            this.dispatchCallback('resized', this.dimensions);
            return this;
        },

        // projecting points on and off screen
        coordinatePoint: function(coord) {
            // Return an x, y point on the map image for a given coordinate.
            if (coord.zoom != this.coordinate.zoom) {
                coord = coord.zoomTo(this.coordinate.zoom);
            }

            // distance from the center of the map
            var point = new MM.Point(this.dimensions.x / 2, this.dimensions.y / 2);
            point.x += this.tileSize.x * (coord.column - this.coordinate.column);
            point.y += this.tileSize.y * (coord.row - this.coordinate.row);

            return point;
        },

        // Get a `MM.Coordinate` from an `MM.Point` - returns a new tile-like object
        // from a screen point.
        pointCoordinate: function(point) {
            // new point coordinate reflecting distance from map center, in tile widths
            var coord = this.coordinate.copy();
            coord.column += (point.x - this.dimensions.x / 2) / this.tileSize.x;
            coord.row += (point.y - this.dimensions.y / 2) / this.tileSize.y;

            return coord;
        },

        // Return an MM.Coordinate (row,col,zoom) for an MM.Location (lat,lon).
        locationCoordinate: function(location) {
            return this.projection.locationCoordinate(location);
        },

        // Return an MM.Location (lat,lon) for an MM.Coordinate (row,col,zoom).
        coordinateLocation: function(coordinate) {
            return this.projection.coordinateLocation(coordinate);
        },

        // Return an x, y point on the map image for a given geographical location.
        locationPoint: function(location) {
            return this.coordinatePoint(this.locationCoordinate(location));
        },

        // Return a geographical location on the map image for a given x, y point.
        pointLocation: function(point) {
            return this.coordinateLocation(this.pointCoordinate(point));
        },

        // inspecting
        getExtent: function() {
            return new MM.Extent(
                this.pointLocation(new MM.Point(0, 0)),
                this.pointLocation(this.dimensions)
            );
        },

        extent: function(locations, precise) {
            if (locations) {
                return this.setExtent(locations, precise);
            } else {
                return this.getExtent();
            }
        },

        // Get the current centerpoint of the map, returning a `Location`
        getCenter: function() {
            return this.projection.coordinateLocation(this.coordinate);
        },

        center: function(location) {
            if (location) {
                return this.setCenter(location);
            } else {
                return this.getCenter();
            }
        },

        // Get the current zoom level of the map, returning a number
        getZoom: function() {
            return this.coordinate.zoom;
        },

        zoom: function(zoom) {
            if (zoom !== undefined) {
                return this.setZoom(zoom);
            } else {
                return this.getZoom();
            }
        },

        // return a copy of the layers array
        getLayers: function() {
            return this.layers.slice();
        },

        // return the first layer with given name
        getLayer: function(name) {
            for (var i = 0; i < this.layers.length; i++) {
                if (name == this.layers[i].name)
                    return this.layers[i];
            }
        },

        // return the layer at the given index
        getLayerAt: function(index) {
            return this.layers[index];
        },

        // put the given layer on top of all the others
        // Since this is called for the first layer, which is by definition
        // added before the map has a valid `coordinate`, we request
        // a redraw only if the map has a center coordinate.
        addLayer: function(layer) {
            this.layers.push(layer);
            this.parent.appendChild(layer.parent);
            layer.map = this; // TODO: remove map property from MM.Layer?
            if (this.coordinate) {
              MM.getFrame(this.getRedraw());
            }
            return this;
        },

        // find the given layer and remove it
        removeLayer: function(layer) {
            for (var i = 0; i < this.layers.length; i++) {
                if (layer == this.layers[i] || layer == this.layers[i].name) {
                    this.removeLayerAt(i);
                    break;
                }
            }
            return this;
        },

        // replace the current layer at the given index with the given layer
        setLayerAt: function(index, layer) {

            if (index < 0 || index >= this.layers.length) {
                throw new Error('invalid index in setLayerAt(): ' + index);
            }

            if (this.layers[index] != layer) {

                // clear existing layer at this index
                if (index < this.layers.length) {
                    var other = this.layers[index];
                    this.parent.insertBefore(layer.parent, other.parent);
                    other.destroy();
                } else {
                // Or if this will be the last layer, it can be simply appended
                    this.parent.appendChild(layer.parent);
                }

                this.layers[index] = layer;
                layer.map = this; // TODO: remove map property from MM.Layer

                MM.getFrame(this.getRedraw());
            }

            return this;
        },

        // put the given layer at the given index, moving others if necessary
        insertLayerAt: function(index, layer) {

            if (index < 0 || index > this.layers.length) {
                throw new Error('invalid index in insertLayerAt(): ' + index);
            }

            if (index == this.layers.length) {
                // it just gets tacked on to the end
                this.layers.push(layer);
                this.parent.appendChild(layer.parent);
            } else {
                // it needs to get slipped in amongst the others
                var other = this.layers[index];
                this.parent.insertBefore(layer.parent, other.parent);
                this.layers.splice(index, 0, layer);
            }

            layer.map = this; // TODO: remove map property from MM.Layer

            MM.getFrame(this.getRedraw());

            return this;
        },

        // remove the layer at the given index, call .destroy() on the layer
        removeLayerAt: function(index) {
            if (index < 0 || index >= this.layers.length) {
                throw new Error('invalid index in removeLayer(): ' + index);
            }

            // gone baby gone.
            var old = this.layers[index];
            this.layers.splice(index, 1);
            old.destroy();

            return this;
        },

        // switch the stacking order of two layers, by index
        swapLayersAt: function(i, j) {

            if (i < 0 || i >= this.layers.length || j < 0 || j >= this.layers.length) {
                throw new Error('invalid index in swapLayersAt(): ' + index);
            }

            var layer1 = this.layers[i],
                layer2 = this.layers[j],
                dummy = document.createElement('div');

            // kick layer2 out, replace it with the dummy.
            this.parent.replaceChild(dummy, layer2.parent);

            // put layer2 back in and kick layer1 out
            this.parent.replaceChild(layer2.parent, layer1.parent);

            // put layer1 back in and ditch the dummy
            this.parent.replaceChild(layer1.parent, dummy);

            // now do it to the layers array
            this.layers[i] = layer2;
            this.layers[j] = layer1;

            return this;
        },

        // Enable and disable layers.
        // Disabled layers are not displayed, are not drawn, and do not request
        // tiles. They do maintain their layer index on the map.
        enableLayer: function(name) {
            var l = this.getLayer(name);
            if (l) l.enable();
            return this;
        },

        enableLayerAt: function(index) {
            var l = this.getLayerAt(index);
            if (l) l.enable();
            return this;
        },

        disableLayer: function(name) {
            var l = this.getLayer(name);
            if (l) l.disable();
            return this;
        },

        disableLayerAt: function(index) {
            var l = this.getLayerAt(index);
            if (l) l.disable();
            return this;
        },


        // limits

        enforceZoomLimits: function(coord) {
            var limits = this.coordLimits;
            if (limits) {
                // clamp zoom level:
                var minZoom = limits[0].zoom;
                var maxZoom = limits[1].zoom;
                if (coord.zoom < minZoom) {
                    coord = coord.zoomTo(minZoom);
                }
                else if (coord.zoom > maxZoom) {
                    coord = coord.zoomTo(maxZoom);
                }
            }
            return coord;
        },

        enforcePanLimits: function(coord) {

            if (this.coordLimits) {

                coord = coord.copy();

                // clamp pan:
                var topLeftLimit = this.coordLimits[0].zoomTo(coord.zoom);
                var bottomRightLimit = this.coordLimits[1].zoomTo(coord.zoom);
                var currentTopLeft = this.pointCoordinate(new MM.Point(0, 0))
                    .zoomTo(coord.zoom);
                var currentBottomRight = this.pointCoordinate(this.dimensions)
                    .zoomTo(coord.zoom);

                // this handles infinite limits:
                // (Infinity - Infinity) is Nan
                // NaN is never less than anything
                if (bottomRightLimit.row - topLeftLimit.row <
                    currentBottomRight.row - currentTopLeft.row) {
                    // if the limit is smaller than the current view center it
                    coord.row = (bottomRightLimit.row + topLeftLimit.row) / 2;
                } else {
                    if (currentTopLeft.row < topLeftLimit.row) {
                        coord.row += topLeftLimit.row - currentTopLeft.row;
                    } else if (currentBottomRight.row > bottomRightLimit.row) {
                        coord.row -= currentBottomRight.row - bottomRightLimit.row;
                    }
                }
                if (bottomRightLimit.column - topLeftLimit.column <
                    currentBottomRight.column - currentTopLeft.column) {
                    // if the limit is smaller than the current view, center it
                    coord.column = (bottomRightLimit.column + topLeftLimit.column) / 2;
                } else {
                    if (currentTopLeft.column < topLeftLimit.column) {
                        coord.column += topLeftLimit.column - currentTopLeft.column;
                    } else if (currentBottomRight.column > bottomRightLimit.column) {
                        coord.column -= currentBottomRight.column - bottomRightLimit.column;
                    }
                }
            }

            return coord;
        },

        // Prevent accidentally navigating outside the `coordLimits` of the map.
        enforceLimits: function(coord) {
            return this.enforcePanLimits(this.enforceZoomLimits(coord));
        },

        // rendering

        // Redraw the tiles on the map, reusing existing tiles.
        draw: function() {
            // make sure we're not too far in or out:
            this.coordinate = this.enforceLimits(this.coordinate);

            // if we don't have dimensions, check the parent size
            if (this.dimensions.x <= 0 || this.dimensions.y <= 0) {
                if (this.autoSize) {
                    // maybe the parent size has changed?
                    var w = this.parent.offsetWidth,
                        h = this.parent.offsetHeight;
                    this.dimensions = new MM.Point(w,h);
                    if (w <= 0 || h <= 0) {
                        return;
                    }
                } else {
                    // the issue can only be corrected with setSize
                    return;
                }
            }

            // draw layers one by one
            for(var i = 0; i < this.layers.length; i++) {
                this.layers[i].draw();
            }

            this.dispatchCallback('drawn');
        },

        _redrawTimer: undefined,

        requestRedraw: function() {
            // we'll always draw within 1 second of this request,
            // sometimes faster if there's already a pending redraw
            // this is used when a new tile arrives so that we clear
            // any parent/child tiles that were only being displayed
            // until the tile loads at the right zoom level
            if (!this._redrawTimer) {
                this._redrawTimer = setTimeout(this.getRedraw(), 1000);
            }
        },

        _redraw: null,

        getRedraw: function() {
            // let's only create this closure once...
            if (!this._redraw) {
                var theMap = this;
                this._redraw = function() {
                    theMap.draw();
                    theMap._redrawTimer = 0;
                };
            }
            return this._redraw;
        },

        // Attempts to destroy all attachment a map has to a page
        // and clear its memory usage.
        destroy: function() {
            for (var j = 0; j < this.layers.length; j++) {
                this.layers[j].destroy();
            }
            this.layers = [];
            this.projection = null;
            for (var i = 0; i < this.eventHandlers.length; i++) {
                this.eventHandlers[i].remove();
            }
            if (this.autoSize) {
                MM.removeEvent(window, 'resize', this.windowResize());
            }
        }
    };
    // Instance of a map intended for drawing to a div.
    //
    //  * `parent` (required DOM element)
    //      Can also be an ID of a DOM element
    //  * `provider` (required MM.MapProvider or URL template)
    //  * `location` (required MM.Location)
    //      Location for map to show
    //  * `zoom` (required number)
    MM.mapByCenterZoom = function(parent, layerish, location, zoom) {
        var layer = MM.coerceLayer(layerish),
            map = new MM.Map(parent, layer, false);
        map.setCenterZoom(location, zoom).draw();
        return map;
    };

    // Instance of a map intended for drawing to a div.
    //
    //  * `parent` (required DOM element)
    //      Can also be an ID of a DOM element
    //  * `provider` (required MM.MapProvider or URL template)
    //  * `locationA` (required MM.Location)
    //      Location of one map corner
    //  * `locationB` (required MM.Location)
    //      Location of other map corner
    MM.mapByExtent = function(parent, layerish, locationA, locationB) {
        var layer = MM.coerceLayer(layerish),
            map = new MM.Map(parent, layer, false);
        map.setExtent([locationA, locationB]).draw();
        return map;
    };
    if (typeof module !== 'undefined' && module.exports) {
      module.exports = {
          Point: MM.Point,
          Projection: MM.Projection,
          MercatorProjection: MM.MercatorProjection,
          LinearProjection: MM.LinearProjection,
          Transformation: MM.Transformation,
          Location: MM.Location,
          MapProvider: MM.MapProvider,
          Template: MM.Template,
          Coordinate: MM.Coordinate,
          deriveTransformation: MM.deriveTransformation
      };
    }
})(MM);

; browserify_shim__define__module__export__(typeof MM != "undefined" ? MM : window.MM);

}).call(global, undefined, undefined, undefined, undefined, function defineExport(ex) { module.exports = ex; });

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{}],2:[function(require,module,exports){
(function (global){
; var __browserify_shim_require__=require;(function browserifyShim(module, exports, require, define, browserify_shim__define__module__export__) {
// Rivets.js
// version: 0.8.1
// author: Michael Richards
// license: MIT
(function() {
  var Rivets, bindMethod, unbindMethod, _ref,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
    __slice = [].slice,
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  Rivets = {
    options: ['prefix', 'templateDelimiters', 'rootInterface', 'preloadData', 'handler'],
    extensions: ['binders', 'formatters', 'components', 'adapters'],
    "public": {
      binders: {},
      components: {},
      formatters: {},
      adapters: {},
      prefix: 'rv',
      templateDelimiters: ['{', '}'],
      rootInterface: '.',
      preloadData: true,
      handler: function(context, ev, binding) {
        return this.call(context, ev, binding.view.models);
      },
      configure: function(options) {
        var descriptor, key, option, value;
        if (options == null) {
          options = {};
        }
        for (option in options) {
          value = options[option];
          if (option === 'binders' || option === 'components' || option === 'formatters' || option === 'adapters') {
            for (key in value) {
              descriptor = value[key];
              Rivets[option][key] = descriptor;
            }
          } else {
            Rivets["public"][option] = value;
          }
        }
      },
      bind: function(el, models, options) {
        var view;
        if (models == null) {
          models = {};
        }
        if (options == null) {
          options = {};
        }
        view = new Rivets.View(el, models, options);
        view.bind();
        return view;
      },
      init: function(component, el, data) {
        var scope, view;
        if (data == null) {
          data = {};
        }
        if (el == null) {
          el = document.createElement('div');
        }
        component = Rivets["public"].components[component];
        el.innerHTML = component.template.call(this, el);
        scope = component.initialize.call(this, el, data);
        view = new Rivets.View(el, scope);
        view.bind();
        return view;
      }
    }
  };

  if (window['jQuery'] || window['$']) {
    _ref = 'on' in jQuery.prototype ? ['on', 'off'] : ['bind', 'unbind'], bindMethod = _ref[0], unbindMethod = _ref[1];
    Rivets.Util = {
      bindEvent: function(el, event, handler) {
        return jQuery(el)[bindMethod](event, handler);
      },
      unbindEvent: function(el, event, handler) {
        return jQuery(el)[unbindMethod](event, handler);
      },
      getInputValue: function(el) {
        var $el;
        $el = jQuery(el);
        if ($el.attr('type') === 'checkbox') {
          return $el.is(':checked');
        } else {
          return $el.val();
        }
      }
    };
  } else {
    Rivets.Util = {
      bindEvent: (function() {
        if ('addEventListener' in window) {
          return function(el, event, handler) {
            return el.addEventListener(event, handler, false);
          };
        }
        return function(el, event, handler) {
          return el.attachEvent('on' + event, handler);
        };
      })(),
      unbindEvent: (function() {
        if ('removeEventListener' in window) {
          return function(el, event, handler) {
            return el.removeEventListener(event, handler, false);
          };
        }
        return function(el, event, handler) {
          return el.detachEvent('on' + event, handler);
        };
      })(),
      getInputValue: function(el) {
        var o, _i, _len, _results;
        if (el.type === 'checkbox') {
          return el.checked;
        } else if (el.type === 'select-multiple') {
          _results = [];
          for (_i = 0, _len = el.length; _i < _len; _i++) {
            o = el[_i];
            if (o.selected) {
              _results.push(o.value);
            }
          }
          return _results;
        } else {
          return el.value;
        }
      }
    };
  }

  Rivets.TypeParser = (function() {
    function TypeParser() {}

    TypeParser.types = {
      primitive: 0,
      keypath: 1
    };

    TypeParser.parse = function(string) {
      if (/^'.*'$|^".*"$/.test(string)) {
        return {
          type: this.types.primitive,
          value: string.slice(1, -1)
        };
      } else if (string === 'true') {
        return {
          type: this.types.primitive,
          value: true
        };
      } else if (string === 'false') {
        return {
          type: this.types.primitive,
          value: false
        };
      } else if (string === 'null') {
        return {
          type: this.types.primitive,
          value: null
        };
      } else if (string === 'undefined') {
        return {
          type: this.types.primitive,
          value: void 0
        };
      } else if (isNaN(Number(string)) === false) {
        return {
          type: this.types.primitive,
          value: Number(string)
        };
      } else {
        return {
          type: this.types.keypath,
          value: string
        };
      }
    };

    return TypeParser;

  })();

  Rivets.TextTemplateParser = (function() {
    function TextTemplateParser() {}

    TextTemplateParser.types = {
      text: 0,
      binding: 1
    };

    TextTemplateParser.parse = function(template, delimiters) {
      var index, lastIndex, lastToken, length, substring, tokens, value;
      tokens = [];
      length = template.length;
      index = 0;
      lastIndex = 0;
      while (lastIndex < length) {
        index = template.indexOf(delimiters[0], lastIndex);
        if (index < 0) {
          tokens.push({
            type: this.types.text,
            value: template.slice(lastIndex)
          });
          break;
        } else {
          if (index > 0 && lastIndex < index) {
            tokens.push({
              type: this.types.text,
              value: template.slice(lastIndex, index)
            });
          }
          lastIndex = index + delimiters[0].length;
          index = template.indexOf(delimiters[1], lastIndex);
          if (index < 0) {
            substring = template.slice(lastIndex - delimiters[1].length);
            lastToken = tokens[tokens.length - 1];
            if ((lastToken != null ? lastToken.type : void 0) === this.types.text) {
              lastToken.value += substring;
            } else {
              tokens.push({
                type: this.types.text,
                value: substring
              });
            }
            break;
          }
          value = template.slice(lastIndex, index).trim();
          tokens.push({
            type: this.types.binding,
            value: value
          });
          lastIndex = index + delimiters[1].length;
        }
      }
      return tokens;
    };

    return TextTemplateParser;

  })();

  Rivets.View = (function() {
    function View(els, models, options) {
      var k, option, v, _base, _i, _j, _len, _len1, _ref1, _ref2, _ref3, _ref4, _ref5;
      this.els = els;
      this.models = models;
      if (options == null) {
        options = {};
      }
      this.update = __bind(this.update, this);
      this.publish = __bind(this.publish, this);
      this.sync = __bind(this.sync, this);
      this.unbind = __bind(this.unbind, this);
      this.bind = __bind(this.bind, this);
      this.select = __bind(this.select, this);
      this.traverse = __bind(this.traverse, this);
      this.build = __bind(this.build, this);
      this.buildBinding = __bind(this.buildBinding, this);
      this.bindingRegExp = __bind(this.bindingRegExp, this);
      this.options = __bind(this.options, this);
      if (!(this.els.jquery || this.els instanceof Array)) {
        this.els = [this.els];
      }
      _ref1 = Rivets.extensions;
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        option = _ref1[_i];
        this[option] = {};
        if (options[option]) {
          _ref2 = options[option];
          for (k in _ref2) {
            v = _ref2[k];
            this[option][k] = v;
          }
        }
        _ref3 = Rivets["public"][option];
        for (k in _ref3) {
          v = _ref3[k];
          if ((_base = this[option])[k] == null) {
            _base[k] = v;
          }
        }
      }
      _ref4 = Rivets.options;
      for (_j = 0, _len1 = _ref4.length; _j < _len1; _j++) {
        option = _ref4[_j];
        this[option] = (_ref5 = options[option]) != null ? _ref5 : Rivets["public"][option];
      }
      this.build();
    }

    View.prototype.options = function() {
      var option, options, _i, _len, _ref1;
      options = {};
      _ref1 = Rivets.extensions.concat(Rivets.options);
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        option = _ref1[_i];
        options[option] = this[option];
      }
      return options;
    };

    View.prototype.bindingRegExp = function() {
      return new RegExp("^" + this.prefix + "-");
    };

    View.prototype.buildBinding = function(binding, node, type, declaration) {
      var context, ctx, dependencies, keypath, options, pipe, pipes;
      options = {};
      pipes = (function() {
        var _i, _len, _ref1, _results;
        _ref1 = declaration.split('|');
        _results = [];
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          pipe = _ref1[_i];
          _results.push(pipe.trim());
        }
        return _results;
      })();
      context = (function() {
        var _i, _len, _ref1, _results;
        _ref1 = pipes.shift().split('<');
        _results = [];
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          ctx = _ref1[_i];
          _results.push(ctx.trim());
        }
        return _results;
      })();
      keypath = context.shift();
      options.formatters = pipes;
      if (dependencies = context.shift()) {
        options.dependencies = dependencies.split(/\s+/);
      }
      return this.bindings.push(new Rivets[binding](this, node, type, keypath, options));
    };

    View.prototype.build = function() {
      var el, parse, _i, _len, _ref1;
      this.bindings = [];
      parse = (function(_this) {
        return function(node) {
          var block, childNode, delimiters, n, parser, text, token, tokens, _i, _j, _len, _len1, _ref1, _results;
          if (node.nodeType === 3) {
            parser = Rivets.TextTemplateParser;
            if (delimiters = _this.templateDelimiters) {
              if ((tokens = parser.parse(node.data, delimiters)).length) {
                if (!(tokens.length === 1 && tokens[0].type === parser.types.text)) {
                  for (_i = 0, _len = tokens.length; _i < _len; _i++) {
                    token = tokens[_i];
                    text = document.createTextNode(token.value);
                    node.parentNode.insertBefore(text, node);
                    if (token.type === 1) {
                      _this.buildBinding('TextBinding', text, null, token.value);
                    }
                  }
                  node.parentNode.removeChild(node);
                }
              }
            }
          } else if (node.nodeType === 1) {
            block = _this.traverse(node);
          }
          if (!block) {
            _ref1 = (function() {
              var _k, _len1, _ref1, _results1;
              _ref1 = node.childNodes;
              _results1 = [];
              for (_k = 0, _len1 = _ref1.length; _k < _len1; _k++) {
                n = _ref1[_k];
                _results1.push(n);
              }
              return _results1;
            })();
            _results = [];
            for (_j = 0, _len1 = _ref1.length; _j < _len1; _j++) {
              childNode = _ref1[_j];
              _results.push(parse(childNode));
            }
            return _results;
          }
        };
      })(this);
      _ref1 = this.els;
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        el = _ref1[_i];
        parse(el);
      }
      this.bindings.sort(function(a, b) {
        var _ref2, _ref3;
        return (((_ref2 = b.binder) != null ? _ref2.priority : void 0) || 0) - (((_ref3 = a.binder) != null ? _ref3.priority : void 0) || 0);
      });
    };

    View.prototype.traverse = function(node) {
      var attribute, attributes, binder, bindingRegExp, block, identifier, regexp, type, value, _i, _j, _len, _len1, _ref1, _ref2, _ref3;
      bindingRegExp = this.bindingRegExp();
      block = node.nodeName === 'SCRIPT' || node.nodeName === 'STYLE';
      _ref1 = node.attributes;
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        attribute = _ref1[_i];
        if (bindingRegExp.test(attribute.name)) {
          type = attribute.name.replace(bindingRegExp, '');
          if (!(binder = this.binders[type])) {
            _ref2 = this.binders;
            for (identifier in _ref2) {
              value = _ref2[identifier];
              if (identifier !== '*' && identifier.indexOf('*') !== -1) {
                regexp = new RegExp("^" + (identifier.replace(/\*/g, '.+')) + "$");
                if (regexp.test(type)) {
                  binder = value;
                }
              }
            }
          }
          binder || (binder = this.binders['*']);
          if (binder.block) {
            block = true;
            attributes = [attribute];
          }
        }
      }
      _ref3 = attributes || node.attributes;
      for (_j = 0, _len1 = _ref3.length; _j < _len1; _j++) {
        attribute = _ref3[_j];
        if (bindingRegExp.test(attribute.name)) {
          type = attribute.name.replace(bindingRegExp, '');
          this.buildBinding('Binding', node, type, attribute.value);
        }
      }
      if (!block) {
        type = node.nodeName.toLowerCase();
        if (this.components[type] && !node._bound) {
          this.bindings.push(new Rivets.ComponentBinding(this, node, type));
          block = true;
        }
      }
      return block;
    };

    View.prototype.select = function(fn) {
      var binding, _i, _len, _ref1, _results;
      _ref1 = this.bindings;
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        binding = _ref1[_i];
        if (fn(binding)) {
          _results.push(binding);
        }
      }
      return _results;
    };

    View.prototype.bind = function() {
      var binding, _i, _len, _ref1, _results;
      _ref1 = this.bindings;
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        binding = _ref1[_i];
        _results.push(binding.bind());
      }
      return _results;
    };

    View.prototype.unbind = function() {
      var binding, _i, _len, _ref1, _results;
      _ref1 = this.bindings;
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        binding = _ref1[_i];
        _results.push(binding.unbind());
      }
      return _results;
    };

    View.prototype.sync = function() {
      var binding, _i, _len, _ref1, _results;
      _ref1 = this.bindings;
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        binding = _ref1[_i];
        _results.push(typeof binding.sync === "function" ? binding.sync() : void 0);
      }
      return _results;
    };

    View.prototype.publish = function() {
      var binding, _i, _len, _ref1, _results;
      _ref1 = this.select(function(b) {
        var _ref1;
        return (_ref1 = b.binder) != null ? _ref1.publishes : void 0;
      });
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        binding = _ref1[_i];
        _results.push(binding.publish());
      }
      return _results;
    };

    View.prototype.update = function(models) {
      var binding, key, model, _i, _len, _ref1, _results;
      if (models == null) {
        models = {};
      }
      for (key in models) {
        model = models[key];
        this.models[key] = model;
      }
      _ref1 = this.bindings;
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        binding = _ref1[_i];
        _results.push(typeof binding.update === "function" ? binding.update(models) : void 0);
      }
      return _results;
    };

    return View;

  })();

  Rivets.Binding = (function() {
    function Binding(view, el, type, keypath, options) {
      this.view = view;
      this.el = el;
      this.type = type;
      this.keypath = keypath;
      this.options = options != null ? options : {};
      this.getValue = __bind(this.getValue, this);
      this.update = __bind(this.update, this);
      this.unbind = __bind(this.unbind, this);
      this.bind = __bind(this.bind, this);
      this.publish = __bind(this.publish, this);
      this.sync = __bind(this.sync, this);
      this.set = __bind(this.set, this);
      this.eventHandler = __bind(this.eventHandler, this);
      this.formattedValue = __bind(this.formattedValue, this);
      this.parseTarget = __bind(this.parseTarget, this);
      this.observe = __bind(this.observe, this);
      this.setBinder = __bind(this.setBinder, this);
      this.formatters = this.options.formatters || [];
      this.dependencies = [];
      this.formatterObservers = {};
      this.model = void 0;
      this.setBinder();
    }

    Binding.prototype.setBinder = function() {
      var identifier, regexp, value, _ref1;
      if (!(this.binder = this.view.binders[this.type])) {
        _ref1 = this.view.binders;
        for (identifier in _ref1) {
          value = _ref1[identifier];
          if (identifier !== '*' && identifier.indexOf('*') !== -1) {
            regexp = new RegExp("^" + (identifier.replace(/\*/g, '.+')) + "$");
            if (regexp.test(this.type)) {
              this.binder = value;
              this.args = new RegExp("^" + (identifier.replace(/\*/g, '(.+)')) + "$").exec(this.type);
              this.args.shift();
            }
          }
        }
      }
      this.binder || (this.binder = this.view.binders['*']);
      if (this.binder instanceof Function) {
        return this.binder = {
          routine: this.binder
        };
      }
    };

    Binding.prototype.observe = function(obj, keypath, callback) {
      return Rivets.sightglass(obj, keypath, callback, {
        root: this.view.rootInterface,
        adapters: this.view.adapters
      });
    };

    Binding.prototype.parseTarget = function() {
      var token;
      token = Rivets.TypeParser.parse(this.keypath);
      if (token.type === 0) {
        return this.value = token.value;
      } else {
        this.observer = this.observe(this.view.models, this.keypath, this.sync);
        return this.model = this.observer.target;
      }
    };

    Binding.prototype.formattedValue = function(value) {
      var ai, arg, args, fi, formatter, id, observer, processedArgs, _base, _i, _j, _len, _len1, _ref1;
      _ref1 = this.formatters;
      for (fi = _i = 0, _len = _ref1.length; _i < _len; fi = ++_i) {
        formatter = _ref1[fi];
        args = formatter.match(/[^\s']+|'([^']|'[^\s])*'|"([^"]|"[^\s])*"/g);
        id = args.shift();
        formatter = this.view.formatters[id];
        args = (function() {
          var _j, _len1, _results;
          _results = [];
          for (_j = 0, _len1 = args.length; _j < _len1; _j++) {
            arg = args[_j];
            _results.push(Rivets.TypeParser.parse(arg));
          }
          return _results;
        })();
        processedArgs = [];
        for (ai = _j = 0, _len1 = args.length; _j < _len1; ai = ++_j) {
          arg = args[ai];
          processedArgs.push(arg.type === 0 ? arg.value : ((_base = this.formatterObservers)[fi] || (_base[fi] = {}), !(observer = this.formatterObservers[fi][ai]) ? (observer = this.observe(this.view.models, arg.value, this.sync), this.formatterObservers[fi][ai] = observer) : void 0, observer.value()));
        }
        if ((formatter != null ? formatter.read : void 0) instanceof Function) {
          value = formatter.read.apply(formatter, [value].concat(__slice.call(processedArgs)));
        } else if (formatter instanceof Function) {
          value = formatter.apply(null, [value].concat(__slice.call(processedArgs)));
        }
      }
      return value;
    };

    Binding.prototype.eventHandler = function(fn) {
      var binding, handler;
      handler = (binding = this).view.handler;
      return function(ev) {
        return handler.call(fn, this, ev, binding);
      };
    };

    Binding.prototype.set = function(value) {
      var _ref1;
      value = value instanceof Function && !this.binder["function"] ? this.formattedValue(value.call(this.model)) : this.formattedValue(value);
      return (_ref1 = this.binder.routine) != null ? _ref1.call(this, this.el, value) : void 0;
    };

    Binding.prototype.sync = function() {
      var dependency, observer;
      return this.set((function() {
        var _i, _j, _len, _len1, _ref1, _ref2, _ref3;
        if (this.observer) {
          if (this.model !== this.observer.target) {
            _ref1 = this.dependencies;
            for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
              observer = _ref1[_i];
              observer.unobserve();
            }
            this.dependencies = [];
            if (((this.model = this.observer.target) != null) && ((_ref2 = this.options.dependencies) != null ? _ref2.length : void 0)) {
              _ref3 = this.options.dependencies;
              for (_j = 0, _len1 = _ref3.length; _j < _len1; _j++) {
                dependency = _ref3[_j];
                observer = this.observe(this.model, dependency, this.sync);
                this.dependencies.push(observer);
              }
            }
          }
          return this.observer.value();
        } else {
          return this.value;
        }
      }).call(this));
    };

    Binding.prototype.publish = function() {
      var args, formatter, id, value, _i, _len, _ref1, _ref2, _ref3;
      if (this.observer) {
        value = this.getValue(this.el);
        _ref1 = this.formatters.slice(0).reverse();
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          formatter = _ref1[_i];
          args = formatter.split(/\s+/);
          id = args.shift();
          if ((_ref2 = this.view.formatters[id]) != null ? _ref2.publish : void 0) {
            value = (_ref3 = this.view.formatters[id]).publish.apply(_ref3, [value].concat(__slice.call(args)));
          }
        }
        return this.observer.setValue(value);
      }
    };

    Binding.prototype.bind = function() {
      var dependency, observer, _i, _len, _ref1, _ref2, _ref3;
      this.parseTarget();
      if ((_ref1 = this.binder.bind) != null) {
        _ref1.call(this, this.el);
      }
      if ((this.model != null) && ((_ref2 = this.options.dependencies) != null ? _ref2.length : void 0)) {
        _ref3 = this.options.dependencies;
        for (_i = 0, _len = _ref3.length; _i < _len; _i++) {
          dependency = _ref3[_i];
          observer = this.observe(this.model, dependency, this.sync);
          this.dependencies.push(observer);
        }
      }
      if (this.view.preloadData) {
        return this.sync();
      }
    };

    Binding.prototype.unbind = function() {
      var ai, args, fi, observer, _i, _len, _ref1, _ref2, _ref3, _ref4;
      if ((_ref1 = this.binder.unbind) != null) {
        _ref1.call(this, this.el);
      }
      if ((_ref2 = this.observer) != null) {
        _ref2.unobserve();
      }
      _ref3 = this.dependencies;
      for (_i = 0, _len = _ref3.length; _i < _len; _i++) {
        observer = _ref3[_i];
        observer.unobserve();
      }
      this.dependencies = [];
      _ref4 = this.formatterObservers;
      for (fi in _ref4) {
        args = _ref4[fi];
        for (ai in args) {
          observer = args[ai];
          observer.unobserve();
        }
      }
      return this.formatterObservers = {};
    };

    Binding.prototype.update = function(models) {
      var _ref1, _ref2;
      if (models == null) {
        models = {};
      }
      this.model = (_ref1 = this.observer) != null ? _ref1.target : void 0;
      return (_ref2 = this.binder.update) != null ? _ref2.call(this, models) : void 0;
    };

    Binding.prototype.getValue = function(el) {
      if (this.binder && (this.binder.getValue != null)) {
        return this.binder.getValue.call(this, el);
      } else {
        return Rivets.Util.getInputValue(el);
      }
    };

    return Binding;

  })();

  Rivets.ComponentBinding = (function(_super) {
    __extends(ComponentBinding, _super);

    function ComponentBinding(view, el, type) {
      var attribute, bindingRegExp, propertyName, _i, _len, _ref1, _ref2;
      this.view = view;
      this.el = el;
      this.type = type;
      this.unbind = __bind(this.unbind, this);
      this.bind = __bind(this.bind, this);
      this.locals = __bind(this.locals, this);
      this.component = this.view.components[this.type];
      this["static"] = {};
      this.observers = {};
      this.upstreamObservers = {};
      bindingRegExp = view.bindingRegExp();
      _ref1 = this.el.attributes || [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        attribute = _ref1[_i];
        if (!bindingRegExp.test(attribute.name)) {
          propertyName = this.camelCase(attribute.name);
          if (__indexOf.call((_ref2 = this.component["static"]) != null ? _ref2 : [], propertyName) >= 0) {
            this["static"][propertyName] = attribute.value;
          } else {
            this.observers[propertyName] = attribute.value;
          }
        }
      }
    }

    ComponentBinding.prototype.sync = function() {};

    ComponentBinding.prototype.update = function() {};

    ComponentBinding.prototype.publish = function() {};

    ComponentBinding.prototype.locals = function() {
      var key, observer, result, value, _ref1, _ref2;
      result = {};
      _ref1 = this["static"];
      for (key in _ref1) {
        value = _ref1[key];
        result[key] = value;
      }
      _ref2 = this.observers;
      for (key in _ref2) {
        observer = _ref2[key];
        result[key] = observer.value();
      }
      return result;
    };

    ComponentBinding.prototype.camelCase = function(string) {
      return string.replace(/-([a-z])/g, function(grouped) {
        return grouped[1].toUpperCase();
      });
    };

    ComponentBinding.prototype.bind = function() {
      var k, key, keypath, observer, option, options, scope, v, _base, _i, _j, _len, _len1, _ref1, _ref2, _ref3, _ref4, _ref5, _ref6, _ref7, _results;
      if (!this.bound) {
        _ref1 = this.observers;
        for (key in _ref1) {
          keypath = _ref1[key];
          this.observers[key] = this.observe(this.view.models, keypath, ((function(_this) {
            return function(key) {
              return function() {
                return _this.componentView.models[key] = _this.observers[key].value();
              };
            };
          })(this)).call(this, key));
        }
        this.bound = true;
      }
      if (this.componentView != null) {
        return this.componentView.bind();
      } else {
        this.el.innerHTML = this.component.template.call(this);
        scope = this.component.initialize.call(this, this.el, this.locals());
        this.el._bound = true;
        options = {};
        _ref2 = Rivets.extensions;
        for (_i = 0, _len = _ref2.length; _i < _len; _i++) {
          option = _ref2[_i];
          options[option] = {};
          if (this.component[option]) {
            _ref3 = this.component[option];
            for (k in _ref3) {
              v = _ref3[k];
              options[option][k] = v;
            }
          }
          _ref4 = this.view[option];
          for (k in _ref4) {
            v = _ref4[k];
            if ((_base = options[option])[k] == null) {
              _base[k] = v;
            }
          }
        }
        _ref5 = Rivets.options;
        for (_j = 0, _len1 = _ref5.length; _j < _len1; _j++) {
          option = _ref5[_j];
          options[option] = (_ref6 = this.component[option]) != null ? _ref6 : this.view[option];
        }
        this.componentView = new Rivets.View(this.el, scope, options);
        this.componentView.bind();
        _ref7 = this.observers;
        _results = [];
        for (key in _ref7) {
          observer = _ref7[key];
          _results.push(this.upstreamObservers[key] = this.observe(this.componentView.models, key, ((function(_this) {
            return function(key, observer) {
              return function() {
                return observer.setValue(_this.componentView.models[key]);
              };
            };
          })(this)).call(this, key, observer)));
        }
        return _results;
      }
    };

    ComponentBinding.prototype.unbind = function() {
      var key, observer, _ref1, _ref2, _ref3;
      _ref1 = this.upstreamObservers;
      for (key in _ref1) {
        observer = _ref1[key];
        observer.unobserve();
      }
      _ref2 = this.observers;
      for (key in _ref2) {
        observer = _ref2[key];
        observer.unobserve();
      }
      return (_ref3 = this.componentView) != null ? _ref3.unbind.call(this) : void 0;
    };

    return ComponentBinding;

  })(Rivets.Binding);

  Rivets.TextBinding = (function(_super) {
    __extends(TextBinding, _super);

    function TextBinding(view, el, type, keypath, options) {
      this.view = view;
      this.el = el;
      this.type = type;
      this.keypath = keypath;
      this.options = options != null ? options : {};
      this.sync = __bind(this.sync, this);
      this.formatters = this.options.formatters || [];
      this.dependencies = [];
      this.formatterObservers = {};
    }

    TextBinding.prototype.binder = {
      routine: function(node, value) {
        return node.data = value != null ? value : '';
      }
    };

    TextBinding.prototype.sync = function() {
      return TextBinding.__super__.sync.apply(this, arguments);
    };

    return TextBinding;

  })(Rivets.Binding);

  Rivets["public"].binders.text = function(el, value) {
    if (el.textContent != null) {
      return el.textContent = value != null ? value : '';
    } else {
      return el.innerText = value != null ? value : '';
    }
  };

  Rivets["public"].binders.html = function(el, value) {
    return el.innerHTML = value != null ? value : '';
  };

  Rivets["public"].binders.show = function(el, value) {
    return el.style.display = value ? '' : 'none';
  };

  Rivets["public"].binders.hide = function(el, value) {
    return el.style.display = value ? 'none' : '';
  };

  Rivets["public"].binders.enabled = function(el, value) {
    return el.disabled = !value;
  };

  Rivets["public"].binders.disabled = function(el, value) {
    return el.disabled = !!value;
  };

  Rivets["public"].binders.checked = {
    publishes: true,
    priority: 2000,
    bind: function(el) {
      return Rivets.Util.bindEvent(el, 'change', this.publish);
    },
    unbind: function(el) {
      return Rivets.Util.unbindEvent(el, 'change', this.publish);
    },
    routine: function(el, value) {
      var _ref1;
      if (el.type === 'radio') {
        return el.checked = ((_ref1 = el.value) != null ? _ref1.toString() : void 0) === (value != null ? value.toString() : void 0);
      } else {
        return el.checked = !!value;
      }
    }
  };

  Rivets["public"].binders.unchecked = {
    publishes: true,
    priority: 2000,
    bind: function(el) {
      return Rivets.Util.bindEvent(el, 'change', this.publish);
    },
    unbind: function(el) {
      return Rivets.Util.unbindEvent(el, 'change', this.publish);
    },
    routine: function(el, value) {
      var _ref1;
      if (el.type === 'radio') {
        return el.checked = ((_ref1 = el.value) != null ? _ref1.toString() : void 0) !== (value != null ? value.toString() : void 0);
      } else {
        return el.checked = !value;
      }
    }
  };

  Rivets["public"].binders.value = {
    publishes: true,
    priority: 3000,
    bind: function(el) {
      if (!(el.tagName === 'INPUT' && el.type === 'radio')) {
        this.event = el.tagName === 'SELECT' ? 'change' : 'input';
        return Rivets.Util.bindEvent(el, this.event, this.publish);
      }
    },
    unbind: function(el) {
      if (!(el.tagName === 'INPUT' && el.type === 'radio')) {
        return Rivets.Util.unbindEvent(el, this.event, this.publish);
      }
    },
    routine: function(el, value) {
      var o, _i, _len, _ref1, _ref2, _ref3, _results;
      if (el.tagName === 'INPUT' && el.type === 'radio') {
        return el.setAttribute('value', value);
      } else if (window.jQuery != null) {
        el = jQuery(el);
        if ((value != null ? value.toString() : void 0) !== ((_ref1 = el.val()) != null ? _ref1.toString() : void 0)) {
          return el.val(value != null ? value : '');
        }
      } else {
        if (el.type === 'select-multiple') {
          if (value != null) {
            _results = [];
            for (_i = 0, _len = el.length; _i < _len; _i++) {
              o = el[_i];
              _results.push(o.selected = (_ref2 = o.value, __indexOf.call(value, _ref2) >= 0));
            }
            return _results;
          }
        } else if ((value != null ? value.toString() : void 0) !== ((_ref3 = el.value) != null ? _ref3.toString() : void 0)) {
          return el.value = value != null ? value : '';
        }
      }
    }
  };

  Rivets["public"].binders["if"] = {
    block: true,
    priority: 4000,
    bind: function(el) {
      var attr, declaration;
      if (this.marker == null) {
        attr = [this.view.prefix, this.type].join('-').replace('--', '-');
        declaration = el.getAttribute(attr);
        this.marker = document.createComment(" rivets: " + this.type + " " + declaration + " ");
        this.bound = false;
        el.removeAttribute(attr);
        el.parentNode.insertBefore(this.marker, el);
        return el.parentNode.removeChild(el);
      }
    },
    unbind: function() {
      var _ref1;
      return (_ref1 = this.nested) != null ? _ref1.unbind() : void 0;
    },
    routine: function(el, value) {
      var key, model, models, _ref1;
      if (!!value === !this.bound) {
        if (value) {
          models = {};
          _ref1 = this.view.models;
          for (key in _ref1) {
            model = _ref1[key];
            models[key] = model;
          }
          (this.nested || (this.nested = new Rivets.View(el, models, this.view.options()))).bind();
          this.marker.parentNode.insertBefore(el, this.marker.nextSibling);
          return this.bound = true;
        } else {
          el.parentNode.removeChild(el);
          this.nested.unbind();
          return this.bound = false;
        }
      }
    },
    update: function(models) {
      var _ref1;
      return (_ref1 = this.nested) != null ? _ref1.update(models) : void 0;
    }
  };

  Rivets["public"].binders.unless = {
    block: true,
    priority: 4000,
    bind: function(el) {
      return Rivets["public"].binders["if"].bind.call(this, el);
    },
    unbind: function() {
      return Rivets["public"].binders["if"].unbind.call(this);
    },
    routine: function(el, value) {
      return Rivets["public"].binders["if"].routine.call(this, el, !value);
    },
    update: function(models) {
      return Rivets["public"].binders["if"].update.call(this, models);
    }
  };

  Rivets["public"].binders['on-*'] = {
    "function": true,
    priority: 1000,
    unbind: function(el) {
      if (this.handler) {
        return Rivets.Util.unbindEvent(el, this.args[0], this.handler);
      }
    },
    routine: function(el, value) {
      if (this.handler) {
        Rivets.Util.unbindEvent(el, this.args[0], this.handler);
      }
      return Rivets.Util.bindEvent(el, this.args[0], this.handler = this.eventHandler(value));
    }
  };

  Rivets["public"].binders['each-*'] = {
    block: true,
    priority: 4000,
    bind: function(el) {
      var attr, view, _i, _len, _ref1;
      if (this.marker == null) {
        attr = [this.view.prefix, this.type].join('-').replace('--', '-');
        this.marker = document.createComment(" rivets: " + this.type + " ");
        this.iterated = [];
        el.removeAttribute(attr);
        el.parentNode.insertBefore(this.marker, el);
        el.parentNode.removeChild(el);
      } else {
        _ref1 = this.iterated;
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          view = _ref1[_i];
          view.bind();
        }
      }
    },
    unbind: function(el) {
      var view, _i, _len, _ref1, _results;
      if (this.iterated != null) {
        _ref1 = this.iterated;
        _results = [];
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          view = _ref1[_i];
          _results.push(view.unbind());
        }
        return _results;
      }
    },
    routine: function(el, collection) {
      var binding, data, i, index, key, model, modelName, options, previous, template, view, _i, _j, _k, _len, _len1, _len2, _ref1, _ref2, _ref3, _results;
      modelName = this.args[0];
      collection = collection || [];
      if (this.iterated.length > collection.length) {
        _ref1 = Array(this.iterated.length - collection.length);
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          i = _ref1[_i];
          view = this.iterated.pop();
          view.unbind();
          this.marker.parentNode.removeChild(view.els[0]);
        }
      }
      for (index = _j = 0, _len1 = collection.length; _j < _len1; index = ++_j) {
        model = collection[index];
        data = {
          index: index
        };
        data[modelName] = model;
        if (this.iterated[index] == null) {
          _ref2 = this.view.models;
          for (key in _ref2) {
            model = _ref2[key];
            if (data[key] == null) {
              data[key] = model;
            }
          }
          previous = this.iterated.length ? this.iterated[this.iterated.length - 1].els[0] : this.marker;
          options = this.view.options();
          options.preloadData = true;
          template = el.cloneNode(true);
          view = new Rivets.View(template, data, options);
          view.bind();
          this.iterated.push(view);
          this.marker.parentNode.insertBefore(template, previous.nextSibling);
        } else if (this.iterated[index].models[modelName] !== model) {
          this.iterated[index].update(data);
        }
      }
      if (el.nodeName === 'OPTION') {
        _ref3 = this.view.bindings;
        _results = [];
        for (_k = 0, _len2 = _ref3.length; _k < _len2; _k++) {
          binding = _ref3[_k];
          if (binding.el === this.marker.parentNode && binding.type === 'value') {
            _results.push(binding.sync());
          } else {
            _results.push(void 0);
          }
        }
        return _results;
      }
    },
    update: function(models) {
      var data, key, model, view, _i, _len, _ref1, _results;
      data = {};
      for (key in models) {
        model = models[key];
        if (key !== this.args[0]) {
          data[key] = model;
        }
      }
      _ref1 = this.iterated;
      _results = [];
      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
        view = _ref1[_i];
        _results.push(view.update(data));
      }
      return _results;
    }
  };

  Rivets["public"].binders['class-*'] = function(el, value) {
    var elClass;
    elClass = " " + el.className + " ";
    if (!value === (elClass.indexOf(" " + this.args[0] + " ") !== -1)) {
      return el.className = value ? "" + el.className + " " + this.args[0] : elClass.replace(" " + this.args[0] + " ", ' ').trim();
    }
  };

  Rivets["public"].binders['*'] = function(el, value) {
    if (value != null) {
      return el.setAttribute(this.type, value);
    } else {
      return el.removeAttribute(this.type);
    }
  };

  Rivets["public"].adapters['.'] = {
    id: '_rv',
    counter: 0,
    weakmap: {},
    weakReference: function(obj) {
      var id, _base, _name;
      if (!obj.hasOwnProperty(this.id)) {
        id = this.counter++;
        Object.defineProperty(obj, this.id, {
          value: id
        });
      }
      return (_base = this.weakmap)[_name = obj[this.id]] || (_base[_name] = {
        callbacks: {}
      });
    },
    cleanupWeakReference: function(ref, id) {
      if (!Object.keys(ref.callbacks).length) {
        if (!(ref.pointers && Object.keys(ref.pointers).length)) {
          return delete this.weakmap[id];
        }
      }
    },
    stubFunction: function(obj, fn) {
      var map, original, weakmap;
      original = obj[fn];
      map = this.weakReference(obj);
      weakmap = this.weakmap;
      return obj[fn] = function() {
        var callback, k, r, response, _i, _len, _ref1, _ref2, _ref3, _ref4;
        response = original.apply(obj, arguments);
        _ref1 = map.pointers;
        for (r in _ref1) {
          k = _ref1[r];
          _ref4 = (_ref2 = (_ref3 = weakmap[r]) != null ? _ref3.callbacks[k] : void 0) != null ? _ref2 : [];
          for (_i = 0, _len = _ref4.length; _i < _len; _i++) {
            callback = _ref4[_i];
            callback();
          }
        }
        return response;
      };
    },
    observeMutations: function(obj, ref, keypath) {
      var fn, functions, map, _base, _i, _len;
      if (Array.isArray(obj)) {
        map = this.weakReference(obj);
        if (map.pointers == null) {
          map.pointers = {};
          functions = ['push', 'pop', 'shift', 'unshift', 'sort', 'reverse', 'splice'];
          for (_i = 0, _len = functions.length; _i < _len; _i++) {
            fn = functions[_i];
            this.stubFunction(obj, fn);
          }
        }
        if ((_base = map.pointers)[ref] == null) {
          _base[ref] = [];
        }
        if (__indexOf.call(map.pointers[ref], keypath) < 0) {
          return map.pointers[ref].push(keypath);
        }
      }
    },
    unobserveMutations: function(obj, ref, keypath) {
      var idx, map, pointers;
      if (Array.isArray(obj) && (obj[this.id] != null)) {
        if (map = this.weakmap[obj[this.id]]) {
          if (pointers = map.pointers[ref]) {
            if ((idx = pointers.indexOf(keypath)) >= 0) {
              pointers.splice(idx, 1);
            }
            if (!pointers.length) {
              delete map.pointers[ref];
            }
            return this.cleanupWeakReference(map, obj[this.id]);
          }
        }
      }
    },
    observe: function(obj, keypath, callback) {
      var callbacks, desc, value;
      callbacks = this.weakReference(obj).callbacks;
      if (callbacks[keypath] == null) {
        callbacks[keypath] = [];
        desc = Object.getOwnPropertyDescriptor(obj, keypath);
        if (!((desc != null ? desc.get : void 0) || (desc != null ? desc.set : void 0))) {
          value = obj[keypath];
          Object.defineProperty(obj, keypath, {
            enumerable: true,
            get: function() {
              return value;
            },
            set: (function(_this) {
              return function(newValue) {
                var map, _i, _len, _ref1;
                if (newValue !== value) {
                  _this.unobserveMutations(value, obj[_this.id], keypath);
                  value = newValue;
                  if (map = _this.weakmap[obj[_this.id]]) {
                    callbacks = map.callbacks;
                    if (callbacks[keypath]) {
                      _ref1 = callbacks[keypath].slice();
                      for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
                        callback = _ref1[_i];
                        if (__indexOf.call(callbacks[keypath], callback) >= 0) {
                          callback();
                        }
                      }
                    }
                    return _this.observeMutations(newValue, obj[_this.id], keypath);
                  }
                }
              };
            })(this)
          });
        }
      }
      if (__indexOf.call(callbacks[keypath], callback) < 0) {
        callbacks[keypath].push(callback);
      }
      return this.observeMutations(obj[keypath], obj[this.id], keypath);
    },
    unobserve: function(obj, keypath, callback) {
      var callbacks, idx, map;
      if (map = this.weakmap[obj[this.id]]) {
        if (callbacks = map.callbacks[keypath]) {
          if ((idx = callbacks.indexOf(callback)) >= 0) {
            callbacks.splice(idx, 1);
            if (!callbacks.length) {
              delete map.callbacks[keypath];
            }
          }
          this.unobserveMutations(obj[keypath], obj[this.id], keypath);
          return this.cleanupWeakReference(map, obj[this.id]);
        }
      }
    },
    get: function(obj, keypath) {
      return obj[keypath];
    },
    set: function(obj, keypath, value) {
      return obj[keypath] = value;
    }
  };

  Rivets.factory = function(sightglass) {
    Rivets.sightglass = sightglass;
    Rivets["public"]._ = Rivets;
    return Rivets["public"];
  };

  if (typeof (typeof module !== "undefined" && module !== null ? module.exports : void 0) === 'object') {
    module.exports = Rivets.factory(__browserify_shim_require__('sightglass'));
  } else if (typeof define === 'function' && define.amd) {
    define(['sightglass'], function(sightglass) {
      return this.rivets = Rivets.factory(sightglass);
    });
  } else {
    this.rivets = Rivets.factory(sightglass);
  }

}).call(this);

; browserify_shim__define__module__export__(typeof rivets != "undefined" ? rivets : window.rivets);

}).call(global, undefined, undefined, undefined, undefined, function defineExport(ex) { module.exports = ex; });

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{}],3:[function(require,module,exports){
(function (global){
; var __browserify_shim_require__=require;(function browserifyShim(module, exports, require, define, browserify_shim__define__module__export__) {
(function() {
  // Public sightglass interface.
  function sightglass(obj, keypath, callback, options) {
    return new Observer(obj, keypath, callback, options)
  }

  // Batteries not included.
  sightglass.adapters = {}

  // Constructs a new keypath observer and kicks things off.
  function Observer(obj, keypath, callback, options) {
    this.options = options || {}
    this.options.adapters = this.options.adapters || {}
    this.obj = obj
    this.keypath = keypath
    this.callback = callback
    this.objectPath = []
    this.update = this.update.bind(this)
    this.parse()

    if (isObject(this.target = this.realize())) {
      this.set(true, this.key, this.target, this.callback)
    }
  }

  // Tokenizes the provided keypath string into interface + path tokens for the
  // observer to work with.
  Observer.tokenize = function(keypath, interfaces, root) {
    var tokens = []
    var current = {i: root, path: ''}
    var index, chr

    for (index = 0; index < keypath.length; index++) {
      chr = keypath.charAt(index)

      if (!!~interfaces.indexOf(chr)) {
        tokens.push(current)
        current = {i: chr, path: ''}
      } else {
        current.path += chr
      }
    }

    tokens.push(current)
    return tokens
  }

  // Parses the keypath using the interfaces defined on the view. Sets variables
  // for the tokenized keypath as well as the end key.
  Observer.prototype.parse = function() {
    var interfaces = this.interfaces()
    var root, path

    if (!interfaces.length) {
      error('Must define at least one adapter interface.')
    }

    if (!!~interfaces.indexOf(this.keypath[0])) {
      root = this.keypath[0]
      path = this.keypath.substr(1)
    } else {
      if (typeof (root = this.options.root || sightglass.root) === 'undefined') {
        error('Must define a default root adapter.')
      }

      path = this.keypath
    }

    this.tokens = Observer.tokenize(path, interfaces, root)
    this.key = this.tokens.pop()
  }

  // Realizes the full keypath, attaching observers for every key and correcting
  // old observers to any changed objects in the keypath.
  Observer.prototype.realize = function() {
    var current = this.obj
    var unreached = false
    var prev

    this.tokens.forEach(function(token, index) {
      if (isObject(current)) {
        if (typeof this.objectPath[index] !== 'undefined') {
          if (current !== (prev = this.objectPath[index])) {
            this.set(false, token, prev, this.update)
            this.set(true, token, current, this.update)
            this.objectPath[index] = current
          }
        } else {
          this.set(true, token, current, this.update)
          this.objectPath[index] = current
        }

        current = this.get(token, current)
      } else {
        if (unreached === false) {
          unreached = index
        }

        if (prev = this.objectPath[index]) {
          this.set(false, token, prev, this.update)
        }
      }
    }, this)

    if (unreached !== false) {
      this.objectPath.splice(unreached)
    }

    return current
  }

  // Updates the keypath. This is called when any intermediary key is changed.
  Observer.prototype.update = function() {
    var next, oldValue

    if ((next = this.realize()) !== this.target) {
      if (isObject(this.target)) {
        this.set(false, this.key, this.target, this.callback)
      }

      if (isObject(next)) {
        this.set(true, this.key, next, this.callback)
      }

      oldValue = this.value()
      this.target = next

      if (this.value() !== oldValue) this.callback()
    }
  }

  // Reads the current end value of the observed keypath. Returns undefined if
  // the full keypath is unreachable.
  Observer.prototype.value = function() {
    if (isObject(this.target)) {
      return this.get(this.key, this.target)
    }
  }

  // Sets the current end value of the observed keypath. Calling setValue when
  // the full keypath is unreachable is a no-op.
  Observer.prototype.setValue = function(value) {
    if (isObject(this.target)) {
      this.adapter(this.key).set(this.target, this.key.path, value)
    }
  }

  // Gets the provided key on an object.
  Observer.prototype.get = function(key, obj) {
    return this.adapter(key).get(obj, key.path)
  }

  // Observes or unobserves a callback on the object using the provided key.
  Observer.prototype.set = function(active, key, obj, callback) {
    var action = active ? 'observe' : 'unobserve'
    this.adapter(key)[action](obj, key.path, callback)
  }

  // Returns an array of all unique adapter interfaces available.
  Observer.prototype.interfaces = function() {
    var interfaces = Object.keys(this.options.adapters)

    Object.keys(sightglass.adapters).forEach(function(i) {
      if (!~interfaces.indexOf(i)) {
        interfaces.push(i)
      }
    })

    return interfaces
  }

  // Convenience function to grab the adapter for a specific key.
  Observer.prototype.adapter = function(key) {
    return this.options.adapters[key.i] ||
      sightglass.adapters[key.i]
  }

  // Unobserves the entire keypath.
  Observer.prototype.unobserve = function() {
    var obj

    this.tokens.forEach(function(token, index) {
      if (obj = this.objectPath[index]) {
        this.set(false, token, obj, this.update)
      }
    }, this)

    if (isObject(this.target)) {
      this.set(false, this.key, this.target, this.callback)
    }
  }

  // Check if a value is an object than can be observed.
  function isObject(obj) {
    return typeof obj === 'object' && obj !== null
  }

  // Error thrower.
  function error(message) {
    throw new Error('[sightglass] ' + message)
  }

  // Export module for Node and the browser.
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = sightglass
  } else if (typeof define === 'function' && define.amd) {
    define([], function() {
      return this.sightglass = sightglass
    })
  } else {
    this.sightglass = sightglass
  }
}).call(this);

; browserify_shim__define__module__export__(typeof sightglass != "undefined" ? sightglass : window.sightglass);

}).call(global, undefined, undefined, undefined, undefined, function defineExport(ex) { module.exports = ex; });

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{}],4:[function(require,module,exports){
var rivets = require('./vendor/rivets.js'),
    evidence = require('./config/evidence.js'),
    dates = require('./config/dates.js'),
    interfaceText = require('./config/interface-text.js'),
    map = require('./map/map.js'),
    overlayUrl = require('./config/overlay.js').overlayUrl,
    templ = require('./templates/template.js'),
    helper = require('./helpers/helper.js'),
    fullscreen = require('./helpers/fullscreen.js'),
    stylingUI = require('./helpers/styleUI.js'),
    navigationUI = require('./helpers/navigationUI.js'),
    filtersUI = require('./helpers/filtersUI.js'),
    moduleElement,
    stateListener,
    navigation,
    filters,
    styling,
    shared,
    options = {
        type: 'TAXON',
        key: '1'
    };
    
module.exports = (function () {
    "use strict";
    
    function createMap(moduleElement, settings) {
        var mapParentElement,
            mapElement,
            baseMap;
        helper.extend(options, settings);
        createModels();
        
        //set evidence to match params or default
        if (options.cat == 'all') {
            options.cat = evidence.all;
        }
        //set active evidence based on options or defaults
        helper.setFlag({
            obj: filters.evidence.options,
            keys: options.cat ? options.cat : evidence.defaultOptions,
            property: 'active'
        });

        moduleElement.innerHTML = moduleElement.innerHTML + templ.html;
        mapParentElement = moduleElement.querySelector('.gbifBasicMap_mapComponent');
        mapElement = mapParentElement.querySelector('.gbifMapComponent_map');
        if (helper.supportsDisplayValue('flex')) {
            moduleElement.querySelector('.gbifBasicMapWrapper').classList.add('flex');
        }
        try {
            rivets.bind(moduleElement, {
                filters: filters,
                styling: styling,
                shared: shared,
                navigation: navigation,
                lang: interfaceText,
                map: map
            });
        }
        catch (err) {
            console.error(JSON.stringify(err.message));
        }

        //all interaction woth the map should close side navigation overlays
        helper.addEventListener(mapElement, 'click', navigation.hideAll);
        helper.addEventListener(mapElement, 'touchstart', navigation.hideAll);

        baseMap = styling.maps.options[styling.maps.selectedValue];
        map.init({
            basemap: baseMap,
            parentElement: mapParentElement,
            lat: options.lat,
            lng: options.lng,
            zoom: options.zoom,
            geoJson: options.geoJson
        });
        updateOverlay();
        map.addExtentChangeListener(function (extent) {
            if (typeof ga !== 'undefined') {
                var maptype = map.hasOverlay() ? options.type : 'empty';
                ga('send', 'event', 'map', 'map_usage', maptype);
            }
            navigation.hideAll();
            notifyListener(extent);
        });

        //if no zoom has been set and there is no geojson configured, then zoom and pan to a sensible zoom level
        if (!options.zoom && !options.geoJson) {
            helper.getMetaData(settings, updateExtendAndResolution);
        }

        if (!navigation.simplifyInterface) {
            fullscreen.addFullScreenButton({
                widget: moduleElement.querySelector('.gbifBasicMap'),
                button: moduleElement.querySelector('.fullscreen')
            });
        }
    }

    /**
    Build the searchurl with the state of the map. To be used by the iframe parent
    */
    function notifyListener() {
        var extent,
            state,
            evidenceArray,
            evidenceFilter = [];

        if (!stateListener) {
            return;
        }
        //Examples
        //TAXON_KEY=1459&HAS_GEOSPATIAL_ISSUE=false&GEOMETRY=-180+-89%2C-180+90%2C180+90%2C180+-89%2C-180+-89&YEAR=*%2C2020
        //DATASET_KEY=75018539-6328-41de-b875-7c2e61dc1635&HAS_GEOSPATIAL_ISSUE=false&GEOMETRY=-180+-82%2C-180+82%2C180+82%2C180+-82%2C-180+-82&BASIS_OF_RECORD=OBSERVATION&YEAR=*%2C2020
        extent = map.getExtent();
        evidenceArray = filters.evidence.getActiveEvidenceAsArray();
        if (evidenceArray.length != evidence.all.length) {
            evidenceArray.forEach(function (e) {
                e.filterAbbr.forEach(function (abb) {
                    evidenceFilter.push(abb);
                });
            });
        }
        state = {
            HAS_GEOSPATIAL_ISSUE: false,
            BASIS_OF_RECORD: evidenceFilter,
            GEOMETRY: helper.buildVisibleGeometry(extent.north, extent.south, extent.east, extent.west)
        };
        if (!filters.dates.options[0].active) {
            state.YEAR =   filters.dates.options[filters.dates.start].filterAbbr.start + ',' +
                            filters.dates.options[filters.dates.end].filterAbbr.end;
        }
        state[helper.apiTranslate(options.type)] = options.key;//the different APIs use different names. Trnalsate to match
        stateListener(helper.serialiseObject(state, true));
    }

    function updateExtendAndResolution(data) {
        if (data.count > 0) {
            setExtent(data);
        }
        if (data.resolution) {
            styling.size.selectedValue = data.resolution;
            updateOverlay();
        }
    }
    
    /**
    Stitch together settings for new overlay
    */
    function updateOverlay() {
        if (filters.evidence.getActiveEvidenceAsArray().length === 0) {
            map.removeOverlay();
        }else {
            var colors = styling.maps.options[styling.maps.selectedValue]["png-render-style"],
                params = helper.serialiseObject({
                    key: options.key,
                    layer: createFilter(filters),
                    type: options.type || 'TAXON',
                    resolution: styling.size.selectedValue
                });
            map.setOverlay(overlayUrl + '?x={X}&y={Y}&z={Z}&' + colors + '&' + params);
        }
        filters.dates.showDates = isDatedEvidence(filters);

        notifyListener();
    }

    function toggleGeoJsonOverlay() {
        map.toggleGeoJsonOverlay();
    }

    /**
    Creates layers based on a combination of Basis of Record (evidence) and Dates
    */
    function createFilter(filters) {
        var evidence,
            years,
            layer = [];
        //get active evidence and years
        evidence = filters.evidence.getActiveEvidenceAsArray();
        years = filters.dates.options.filter(function (e) {
            return e.active;
        });
        if (filters.dates.undated.active) {
            years.push(filters.dates.undated);
        }
        
        //combine to create layer anmes used by tile service
        evidence.forEach(function (e) {
            if (e.dated) {
                years.forEach(function (d) {
                    layer.push(e.abbr + '_' + d.abbr);
                });
            } else {
                layer.push(e.abbr);
            }
        });
        return layer;
    }

    function isDatedEvidence(filters) {
        var list = helper.filterObjToArray(filters.evidence.options, function (e) {
            return e.active && e.dated;
        });
        return list.length !== 0;
    }

    function createModels() {
        shared = {
            updateOverlay: updateOverlay,
            toggleGeoJsonOverlay: toggleGeoJsonOverlay
        };
        filters = filtersUI(options, shared);
        styling = stylingUI(options);
        navigation = navigationUI(options);
    }

    function setExtent(extent) {
        map.setExtent(extent);
    }
    
    function setStateListener(cb) {
        stateListener = cb;
    }
    
    function addGeoJson(collection) {
        map.addGeoJson(collection);
        if (!navigation.simplifyInterface) {
            styling.information.show = true;
        }
    }
    
    return {
        createMap: createMap,
        setStateListener: setStateListener,
        setExtent: setExtent,
        addGeoJson: addGeoJson
    };
})();

},{"./config/dates.js":6,"./config/evidence.js":7,"./config/interface-text.js":8,"./config/overlay.js":9,"./helpers/filtersUI.js":12,"./helpers/fullscreen.js":13,"./helpers/helper.js":14,"./helpers/navigationUI.js":15,"./helpers/styleUI.js":16,"./map/map.js":18,"./templates/template.js":21,"./vendor/rivets.js":23}],5:[function(require,module,exports){
module.exports = {
    defaultOption: 'classic',
    options: {
        'classic': {
            "name": "Classic",
            "url": "https://tile.gbif.org/3857/omt/{Z}/{X}/{Y}@1x.png?style=gbif-classic",
            "attribution": "© <a href='https://openmaptiles.org/'>OpenMapTiles</a> © <a href='http://www.openstreetmap.org/copyright' target='_blank'>OpenStreetMap contributors</a>",
            "png-render-style": "palette=yellows_reds",
            "subdomains": [],
            "enabled": true
        },
        'dark': {
            "name": "Night",
            "url": "https://tile.gbif.org/3857/omt/{Z}/{X}/{Y}@1x.png?style=gbif-dark",
            "attribution": "© <a href='https://openmaptiles.org/'>OpenMapTiles</a> © <a href='http://www.openstreetmap.org/copyright' target='_blank'>OpenStreetMap contributors</a>",
            "png-render-style": "saturation=true",
            "subdomains": [],
            "enabled": false
        },
        'ocean': {
            "name": "Terrain",
            "url": "http://server.arcgisonline.com/ArcGIS/rest/services/Ocean_Basemap/MapServer/tile/{Z}/{Y}/{X}.png",
            "attribution": "Esri, DeLorme, FAO, USGS, NOAA, GEBCO, IHO-IOC GEBCO, NGS, NIWA",
            "png-render-style": "palette=yellows_reds",
            "subdomains": [],
            "enabled": false
        },
        'satellite': {
            "name": "Satellite",
            "url": "http://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{Z}/{Y}/{X}.png",
            "attribution": "Esri, DeLorme, FAO, NOAA, DigitalGlobe, GeoEye, i-cubed, USDA, USGS, AEX, Getmapping, Aerogrid, IGN, IGP, swisstopo, and the GIS User Community",
            "png-render-style": "palette=yellows_reds",
            "subdomains": [],
            "enabled": false
        },
        'light': {
            "name": "High contrast",
            "url": "https://tile.gbif.org/3857/omt/{Z}/{X}/{Y}@1x.png?style=gbif-light",
            "attribution": "© <a href='https://openmaptiles.org/'>OpenMapTiles</a> © <a href='http://www.openstreetmap.org/copyright' target='_blank'>OpenStreetMap contributors</a>",
            "png-render-style": "colors=%2C%2C%23CC0000FF",
            "subdomains": [],
            "enabled": false
        },
        'grey-blue': {
            "name": "Roads",
            "url": "https://tile.gbif.org/3857/omt/{Z}/{X}/{Y}@1x.png?style=osm-bright",
            "attribution": "© <a href='https://openmaptiles.org/'>OpenMapTiles</a> © <a href='http://www.openstreetmap.org/copyright' target='_blank'>OpenStreetMap contributors</a>",
            "png-render-style": "palette=yellows_reds",
            "subdomains": [],
            "enabled": false
        }
    }
};

},{}],6:[function(require,module,exports){
module.exports = {
    start: 0,
    end: 12,
    undated: {
        active: true,
        text: 'No date',
        abbr: 'NO_YEAR',
        filterAbbr: undefined
    },
    options: [
        {
            active: false,
            text: 'Pre 1900',
            abbr: 'PRE_1900',
            filterAbbr: {start: '*', end: 1900}
        },
        {
            active: false,
            text: '1900s',
            abbr: '1900_1910',
            filterAbbr: {start: 1900, end: 1910}
        },
        {
            active: false,
            text: '1910s',
            abbr: '1910_1920',
            filterAbbr: {start: 1910, end: 1920}
        },
        {
            active: false,
            text: '1920s',
            abbr: '1920_1930',
            filterAbbr: {start: 1920, end: 1930}
        },
        {
            active: false,
            text: '1930s',
            abbr: '1930_1940',
            filterAbbr: {start: 1930, end: 1940}
        },
        {
            active: false,
            text: '1940s',
            abbr: '1940_1950',
            filterAbbr: {start: 1940, end: 1950}
        },
        {
            active: false,
            text: '1950s',
            abbr: '1950_1960',
            filterAbbr: {start: 1950, end: 1960}
        },
        {
            active: false,
            text: '1960s',
            abbr: '1960_1970',
            filterAbbr: {start: 1960, end: 1970}
        },
        {
            active: false,
            text: '1970s',
            abbr: '1970_1980',
            filterAbbr: {start: 1970, end: 1980}
        },
        {
            active: false,
            text: '1980s',
            abbr: '1980_1990',
            filterAbbr: {start: 1980, end: 1990}
        },
        {
            active: false,
            text: '1990s',
            abbr: '1990_2000',
            filterAbbr: {start: 1990, end: 2000}
        },
        {
            active: false,
            text: '2000s',
            abbr: '2000_2010',
            filterAbbr: {start: 2000, end: 2010}
        },
        {
            active: false,
            text: '2010s',
            abbr: '2010_2020',
            filterAbbr: {start: 2010, end: 2020}
        }
    ]
};

},{}],7:[function(require,module,exports){
module.exports = {
    defaultOptions: ['sp', 'obs', 'oth', 'fossil', 'living'],
    all: ['sp', 'obs', 'oth', 'fossil', 'living'],
    options: {
        sp: {
            text: 'Preserved specimen',
            abbr: 'SP',
            filterAbbr: ['PRESERVED_SPECIMEN'],
            dated: true
        },
        obs: {
            text: 'Observation',
            abbr: 'OBS',
            filterAbbr: ['OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION'],
            dated: true
        },
        oth: {
            text: 'Other',
            abbr: 'OTH',
            filterAbbr: ['UNKNOWN', 'MATERIAL_SAMPLE', 'LITERATURE'],
            dated: true
        },
        fossil: {
            text: 'Fossil',
            comment: 'No date',
            abbr: 'FOSSIL',
            filterAbbr: ['FOSSIL_SPECIMEN'],
            dated: false
        },
        living: {
            text: 'Living specimen',
            comment: 'No date',
            abbr: 'LIVING',
            filterAbbr: ['LIVING_SPECIMEN'],
            dated: false
        }
    }
};

},{}],8:[function(require,module,exports){
module.exports = {
    filters: {
        headline: 'Filters',
        evidence: {
            headline: 'Basis of record',
            includeUndated: {
                text: 'Show undated',
                description: 'Show occurences without date. Fossils and Living Specimen are never dated.'
            }
        },
        dates: {
            headline: 'Date interval'
        }
    },
    styling: {
        headline: 'Styling',
        basemap: {
            headline: 'Map'
        },
        pointsize: {
            headline: 'Point size'
        },
        overlay: {
            headline: 'Context',
            showGeoJson: 'Show coverage'
        }
    },
    navigation: {
        fullscreen: 'Fullscreen'
    },
    map: {
        attribution: 'Attribution & Disclaimer'
    }
};


},{}],9:[function(require,module,exports){
module.exports = (function () {
    var baseUrlForTesting = document.baseUrlForTesting || '';

    return {
        overlayUrl: baseUrlForTesting + 'density/tile.png',
        jsonUrlTemplate: baseUrlForTesting + 'density/tile.json?key={key}&resolution=1&x=0&y=0&z=0&type={type}'
    };
})();

},{}],10:[function(require,module,exports){
module.exports = {
    defaultOption: 0,
    options: [
        {
            "name": "Small",
            "resolution": 1
        },
        {
            "name": "Medium",
            "resolution": 2
        },
        {
            "name": "Large",
            "resolution": 4
        },
        {
            "name": "X-Large",
            "resolution": 8
        }
    ]
};

},{}],11:[function(require,module,exports){
/*\
|*|
|*|  :: cookies.js ::
|*|
|*|  A complete cookies reader/writer framework with full unicode support.
|*|
|*|  Revision #1 - September 4, 2014
|*|
|*|  https://developer.mozilla.org/en-US/docs/Web/API/document.cookie
|*|  https://developer.mozilla.org/User:fusionchess
|*|
|*|  This framework is released under the GNU Public License, version 3 or later.
|*|  http://www.gnu.org/licenses/gpl-3.0-standalone.html
|*|
|*|  Syntaxes:
|*|
|*|  * docCookies.setItem(name, value[, end[, path[, domain[, secure]]]])
|*|  * docCookies.getItem(name)
|*|  * docCookies.removeItem(name[, path[, domain]])
|*|  * docCookies.hasItem(name)
|*|  * docCookies.keys()
|*|
\*/

module.exports = {
    getItem: function (sKey) {
        if (!sKey) {
            return null;
        }
        return decodeURIComponent(document.cookie.replace(new RegExp("(?:(?:^|.*;)\\s*" + encodeURIComponent(sKey).replace(/[\-\.\+\*]/g, "\\$&") + "\\s*\\=\\s*([^;]*).*$)|^.*$"), "$1")) || null;
    },
    setItem: function (sKey, sValue, vEnd, sPath, sDomain, bSecure) {
        if (!sKey || /^(?:expires|max\-age|path|domain|secure)$/i.test(sKey)) {
            return false;
        }
        var sExpires = "";
        if (vEnd) {
            switch (vEnd.constructor) {
                case Number:
                    sExpires = vEnd === Infinity ? "; expires=Fri, 31 Dec 9999 23:59:59 GMT" : "; max-age=" + vEnd;
                    break;
                case String:
                    sExpires = "; expires=" + vEnd;
                    break;
                case Date:
                    sExpires = "; expires=" + vEnd.toUTCString();
                    break;
            }
        }
        document.cookie = encodeURIComponent(sKey) + "=" + encodeURIComponent(sValue) + sExpires + (sDomain ? "; domain=" + sDomain : "") + (sPath ? "; path=" + sPath : "") + (bSecure ? "; secure" : "");
        return true;
    },
    removeItem: function (sKey, sPath, sDomain) {
        if (!this.hasItem(sKey)) {
            return false;
        }
        document.cookie = encodeURIComponent(sKey) + "=; expires=Thu, 01 Jan 1970 00:00:00 GMT" + (sDomain ? "; domain=" + sDomain : "") + (sPath ? "; path=" + sPath : "");
        return true;
    },
    hasItem: function (sKey) {
        if (!sKey) {
            return false;
        }
        return (new RegExp("(?:^|;\\s*)" + encodeURIComponent(sKey).replace(/[\-\.\+\*]/g, "\\$&") + "\\s*\\=")).test(document.cookie);
    },
    keys: function () {
        var aKeys = document.cookie.replace(/((?:^|\s*;)[^\=]+)(?=;|$)|^\s*|\s*(?:\=[^;]*)?(?:\1|$)/g, "").split(/\s*(?:\=[^;]*)?;\s*/);
        for (var nLen = aKeys.length, nIdx = 0; nIdx < nLen; nIdx++) {
            aKeys[nIdx] = decodeURIComponent(aKeys[nIdx]);
        }
        return aKeys;
    }
};

},{}],12:[function(require,module,exports){
var helper = require('./helper.js'),
    dates = require('../config/dates.js'),
    evidence = require('../config/evidence.js'),
    NO_TOUCH = false,
    TOUCH = true;

module.exports = function (options, shared) {
    
    function dragStart(e, c) {
        var diffStart = c.index - filters.dates.start,
            diffEnd = filters.dates.end - c.index;
        filters.dates.drag.dragging = true;

        if (filters.dates.start > filters.dates.end) {
            c.filters.dates.start = c.index;
            c.filters.dates.end = c.index;
            updateActiveDates(filters);
            return true;
        }

        e.preventDefault();

        filters.dates.drag.draggingStart = false;
        filters.dates.drag.draggingBoth = false;
        if (c.index == filters.dates.start && filters.dates.start == filters.dates.end) {
            filters.dates.end = c.index - 1;
        } else if (Math.abs(diffStart) == Math.abs(diffEnd)) {
            if (diffStart < 0) {
                filters.dates.start = c.index;
                filters.dates.drag.draggingStart = true;
            } else {
                filters.dates.end = c.index;
            }
        } else if (Math.abs(diffStart) < Math.abs(diffEnd)) {
            filters.dates.drag.draggingStart = true;
            if (diffStart === 0) {
                filters.dates.start = c.index + 1;
            } else {
                filters.dates.start = c.index;
            }
        } else {
            if (diffEnd === 0) {
                filters.dates.end = c.index - 1;
            } else {
                filters.dates.end = c.index;
            }
        }

        updateActiveDates(filters);
    }
    
    function touchmove(event, context) {
        var d = filters.dates.drag,
            element,
            index;
        if (d.dragging) {
            element = document.elementFromPoint(event.touches[0].pageX, event.touches[0].pageY);
            index = parseInt(element.getAttribute('index'));
            mouseenter(event, {index: index});
        }
    }
    
    function mouseenter(e, c) {
        var d = filters.dates.drag,
            diffStart,
            diffEnd;
        if (d.dragging) {
            diffStart = c.index - filters.dates.start;
            diffEnd = filters.dates.end - c.index;

            if (filters.dates.drag.draggingBoth) {
                filters.dates.start = c.index;
                filters.dates.end = c.index;
            } else if (filters.dates.drag.draggingStart) {
                filters.dates.start = Math.min(filters.dates.end, c.index);
            } else {
                filters.dates.end = Math.max(c.index, filters.dates.start);
            }

            updateActiveDates(filters);
        }
    }
    
    function mouseup(event) {
        var d = filters.dates.drag;
        if (d.dragging) {
            d.dragging = false;
            shared.updateOverlay();
            if (typeof ga !== 'undefined') {
                ga('send', 'event', 'map_dates', filters.dates.start, filters.dates.end - filters.dates.start);
            }
        }
    }
    helper.addEventListener(window, 'mouseup', mouseup);
    helper.addEventListener(window, 'touchend', mouseup);
    
    function updateActiveDates(filters) {
        var options = filters.dates.options,
            i;
        filters.dates.undated.active = false;
        for (i = 0; i < options.length; i++) {
            if (i < filters.dates.start) {
                options[i].active = false;
            }else if (i > filters.dates.end) {
                options[i].active = false;
            }else {
                options[i].active = true;
            }

            if (i == filters.dates.start) {
                options[i].endpoint = true;
            } else if (i == filters.dates.end) {
                options[i].endpoint = true;
            }else {
                options[i].endpoint = false;
            }
        }
    }
    
    function selectEvidence(event, context) {
        if (typeof ga !== 'undefined') {
            var act = context.option.active ? 'deselect' : 'select';
            ga('send', 'event', 'map_evidence', act, context.option.abbr);
        }
        context.option.active = !context.option.active;
        context.shared.updateOverlay();
    }
    
    function toggleUndated(event, context) {
        if (typeof ga !== 'undefined') {
            var act = context.filters.dates.undated.active ? 'deselect' : 'select';
            ga('send', 'event', 'map_evidence', act, 'show undated');
        }
        context.filters.dates.undated.active = !context.filters.dates.undated.active;
        context.shared.updateOverlay();
    }
    
    function toggleEvidenceUndatedDesc(event, context) {
        context.filters.undated.showDescription = !context.filters.undated.showDescription;
        return false;
    }
    
    function getActiveEvidenceAsArray() {
        return helper.filterObjToArray(filters.evidence.options, function (e) {
            return e.active;
        });
    }
    
    filters = {
        undated: {
            toggleUndatedMouse: helper.ghostClickWrap(toggleUndated, NO_TOUCH),
            toggleUndatedTouch: helper.ghostClickWrap(toggleUndated, TOUCH),
            showDescription: false,
            toggleVisibilityMouse: helper.ghostClickWrap(toggleEvidenceUndatedDesc, NO_TOUCH),
            toggleVisibilityTouch: helper.ghostClickWrap(toggleEvidenceUndatedDesc, TOUCH)
        },
        dates: {
            undated: dates.undated,
            start: dates.start,
            end: dates.end,
            options: dates.options,
            mouseDragStart: helper.ghostClickWrap(dragStart, NO_TOUCH),
            touchDragStart: helper.ghostClickWrap(dragStart, TOUCH),
            mouseup: mouseup,
            mouseenter: mouseenter,
            touchmove: touchmove,
            drag: {
                dragging: false
            },
            showDates: true
        },
        evidence: {
            options: evidence.options,
            selectMouse: helper.ghostClickWrap(selectEvidence, NO_TOUCH),
            selectTouch: helper.ghostClickWrap(selectEvidence, TOUCH),
            getActiveEvidenceAsArray: getActiveEvidenceAsArray
        }
    };
    updateActiveDates(filters);
    return filters;
};

},{"../config/dates.js":6,"../config/evidence.js":7,"./helper.js":14}],13:[function(require,module,exports){
var helper = require('./helper.js');
module.exports = (function () {
    
    function launchIntoFullscreen(element) {
        if (element.requestFullscreen) {
            element.requestFullscreen();
        } else if (element.mozRequestFullScreen) {
            element.mozRequestFullScreen();
        } else if (element.webkitRequestFullscreen) {
            element.webkitRequestFullscreen();
        } else if (element.msRequestFullscreen) {
            element.msRequestFullscreen();
        }
    }

    /*
    function exitFullscreen() {
        if (document.exitFullscreen) {
            document.exitFullscreen();
        } else if (document.mozCancelFullScreen) {
            document.mozCancelFullScreen();
        } else if (document.webkitExitFullscreen) {
            document.webkitExitFullscreen();
        }
    }
    
    
    function isFullscreen(){
        return !window.screenTop && !window.screenY;
    }
    */
    
    function addFullScreenButton(opt) {
        var widget = opt.widget,
            button = opt.button;
        
        
        function enterFullscreen() {
            if (ga) {
                ga('send', 'event', 'map', 'fullscreen');
            }
            launchIntoFullscreen(widget);
        }

        
        helper.addEventListener(button, 'click', enterFullscreen);
    }
    
    return {
        addFullScreenButton: addFullScreenButton //{widget, button}
    };
})();

},{"./helper.js":14}],14:[function(require,module,exports){
module.exports = (function () {
    var url = require('../config/overlay.js').jsonUrlTemplate,
        scale = 2,
        supportsTouch = 'ontouchstart' in window || navigator.msMaxTouchPoints;

    function callAjax(url, callback) {
        try {
            var xmlhttp;
            // compatible with IE7+, Firefox, Chrome, Opera, Safari
            xmlhttp = new XMLHttpRequest();
            xmlhttp.onreadystatechange = function () {
                if (xmlhttp.readyState == XMLHttpRequest.DONE && xmlhttp.status == 200) {
                    callback(xmlhttp.responseText);
                }
            };
            xmlhttp.open("GET", url, true);
            xmlhttp.send();
        } catch (err) {
            console.error('Could not get meta data from server.');
        }
    }

    function addEventListener(element, eventType, func) {
        try {
            if (element === null) {
                return;
            }
            element.addEventListener(eventType, func);
        }
        catch (err) {
            console.error('Not supported in legacy browsers. ' + err + " Element: " + element);
        }
    }

    function getEstimatedResolution(metaData) {
        if (metaData.count === 0) {
            return 1;
        }
        var width = metaData.maximumLatitude - metaData.minimumLatitude,
            height = metaData.maximumLongitude - metaData.minimumLongitude,
            density = width * height / metaData.count,
            concentration = 3 - 3 / (1 + density / scale),
            res = Math.pow(2, Math.round(concentration));
        return res;
    }

    function extend(a, b) {
        for (var attrname in b) {
            if (b.hasOwnProperty(attrname) && typeof b[attrname] !== 'undefined' && b[attrname] !== null) {
                a[attrname] = b[attrname];
            }
        }
        return a;
    }

    function getMetaData(settings, cb) {
        url = url
            .replace('{key}', settings.key)
            .replace('{type}', settings.type);
        callAjax(url, function (response) {
            var data = JSON.parse(response);
            data.minimumLatitude = Math.max(-90, data.minimumLatitude - 2);
            data.minimumLongitude -= 2;
            data.maximumLatitude = Math.min(90, data.maximumLatitude + 2);
            data.maximumLongitude += 2;
            if (!settings.resolution) {
                data.resolution = getEstimatedResolution(data);
            }
            cb(data);
        });
    }

    function filterObjToArray(obj, predicate) {
        var result = [], key;
        for (key in obj) {
            if (obj.hasOwnProperty(key) && predicate(obj[key])) {
                result.push(obj[key]);
            }
        }
        return result;
    }

    function setFlag(settings) {
        var key;
        for (key in settings.obj) {
            if (settings.obj.hasOwnProperty(key)) {
                settings.obj[key][settings.property] = settings.keys.indexOf(key) > -1;
            }
        }
        return settings.obj;
    }

    /**
    Simple wrapper to prevent ghost clicks
    */
    function ghostClickWrap(func, isTouchEvent) {
        return function () {
            if (isTouchEvent ? !supportsTouch : supportsTouch) {
                return;
            }
            func.apply(this, arguments);
        };
    }

    function serialiseObject(obj, encode) {
        var pairs = [];

        function addParam(prop, val) {
            val = encode ? encodeURIComponent(val) : val;
            pairs.push(prop + '=' + val);
        }
        for (var prop in obj) {
            if (!obj.hasOwnProperty(prop)) {
                continue;
            }
            if (Object.prototype.toString.call(obj[prop]) === '[object Array]') {
                for (var i = 0; i < obj[prop].length; i++) {
                    addParam(prop, obj[prop][i]);
                }
            } else {
                addParam(prop, obj[prop]);
            }
        }
        return pairs.join('&');
    }

    function supportsDisplayValue(val) {
        // detect CSS display:val support in JavaScript
        try {
            var detector = document.createElement("detect");
            detector.style.display = val;
            if (detector.style.display === val) {
                return true;
            } else {
                return false;
            }
        } catch (err) {
            return false;
        }
    }

    function apiTranslate(type) {
        switch (type) {
            case 'TAXON':
                return 'TAXON_KEY';
            case 'DATASET':
                return 'DATASET_KEY';
            default:
                return type;
        }
    }

    /**
     * Builds the geometry from the visible extents of the map, suitable for use in the GBIF search.
     */
    function buildVisibleGeometry(n, s, e, w) {
        function normalize(c) {
            while (c < -180) {
                c += 360;
            }
            while (c > 180) {
                c -= 360;
            }
            return c.toFixed(2);
        }
        e = normalize(e);
        w = normalize(w);
        n = normalize(n);
        s = normalize(s);
        var tmpl = '{w} {s},{w} {n},{e} {n},{e} {s},{w} {s}',
            res = tmpl.replace(/{n}/g, n)
            .replace(/{e}/g, e)
            .replace(/{s}/g, s)
            .replace(/{w}/g, w);
        return res;
    }

    return {
        extend: extend,
        getMetaData: getMetaData,
        serialiseObject: serialiseObject,
        apiTranslate: apiTranslate,
        buildVisibleGeometry: buildVisibleGeometry,
        addEventListener: addEventListener,
        filterObjToArray: filterObjToArray,
        setFlag: setFlag,
        ghostClickWrap: ghostClickWrap,
        supportsTouch: supportsTouch,
        supportsDisplayValue: supportsDisplayValue
    };
})();

},{"../config/overlay.js":9}],15:[function(require,module,exports){
var helper = require('./helper.js'),
    NO_TOUCH = false,
    TOUCH = true;

module.exports = function (options) {
    
    function toggleEvidence(event, context) {
        context.navigation.evidence = !context.navigation.evidence;
    }
    
    function supportsFullscreen() {
        var device = {};
        device.ie9 = /MSIE 9/i.test(navigator.userAgent);
        device.ie10 = /MSIE 10/i.test(navigator.userAgent);
        device.ie11 = /rv:11.0/i.test(navigator.userAgent);
        return !(device.ie9 || device.ie10 || device.ie11);
    }
    
    var nav = {
        supportsTouch: helper.supportsTouch,
        simplifyInterface: options.simplifyInterface,
        hideFullScreenButton: options.simplifyInterface || !supportsFullscreen(),
        filters: false,
        styling: false,
        evidence: false,
        showFilters: function (event, context) {
            context.navigation.filters = true;
            context.navigation.styling = false;
        },
        showStyling: function (event, context) {
            context.navigation.filters = false;
            context.navigation.styling = true;
        },
        toggleEvidenceTouch: helper.ghostClickWrap(toggleEvidence, TOUCH),
        toggleEvidenceMouse: helper.ghostClickWrap(toggleEvidence, NO_TOUCH)
    };

    function hideAll() {
        nav.filters = false;
        nav.styling = false;
    }
    nav.hideAll = hideAll;
    return nav;
};

},{"./helper.js":14}],16:[function(require,module,exports){
var baselayers = require('../config/baselayers.js'),
    resolution = require('../config/resolution.js'),
    helper = require('./helper.js'),
    docCookies = require('./cookie.js'),
    NO_TOUCH = false,
    TOUCH = true;

module.exports = function (options) {
    /**
    Returns either the basemap key stored in a cookie or if none,
    the default one descibed in the baseLayers configuration
    */
    function getDefaultMapId() {
        var cookieSettings;
        if (docCookies.hasItem('settings')) {
            cookieSettings = JSON.parse(docCookies.getItem('settings'));
            if (baselayers.options[cookieSettings.baseMapId]) {
                return cookieSettings.baseMapId;
            }
        }
        return baselayers.defaultOption;
    }
    
    function selectMap(event, context) {
        if (typeof ga !== 'undefined') {
            ga('send', 'event', 'map_basemap', 'select', context.option.id);
        }
        context.styling.maps.selectedValue = context.option.id;
        context.map.setBaseMap(context.option);
        docCookies.setItem('settings', JSON.stringify({baseMapId: context.option.id}));//save the prefered basemap in a cookie
        context.shared.updateOverlay(); //since the baselayer defines the color scheme of the overlay
    }
    
    function selectResolution(event, context) {
        if (typeof ga !== 'undefined') {
            ga('send', 'event', 'map_resolution', 'select', context.option.name, context.option.resolution);
        }
        context.styling.size.selectedValue = context.option.resolution;
        context.shared.updateOverlay();
    }

    function toggleOverlay(event, context) {
        context.styling.information.options.geojson.active = !context.styling.information.options.geojson.active;
        context.shared.toggleGeoJsonOverlay();
    }

    var styling = {
        maps: {
            options: baselayers.options,
            selectedValue: baselayers.options[options.style] ? options.style : getDefaultMapId(),
            select: selectMap
        },
        size: {
            options: resolution.options,
            selectedValue: options.resolution || resolution.options[resolution.defaultOption].resolution,
            select: selectResolution
        },
        information: {
            show: false,
            options: {
                geojson: {
                    active: true,
                    click: helper.ghostClickWrap(toggleOverlay, NO_TOUCH),
                    touch: helper.ghostClickWrap(toggleOverlay, TOUCH)
                }
            }
        }
    };
    return styling;
};

},{"../config/baselayers.js":5,"../config/resolution.js":10,"./cookie.js":11,"./helper.js":14}],17:[function(require,module,exports){
var MM = require('modestmaps');

module.exports = (function () {
    function createPolyOverlay(map) {
        var canvas = document.createElement('canvas'),
            ctx = canvas.getContext('2d'),
            geometries = [],
            points = [],
            hidden;
        
        canvas.style.position = 'absolute';
        canvas.style.left = '0px';
        canvas.style.top = '0px';
        canvas.width = map.dimensions.x;
        canvas.height = map.dimensions.y;
        map.parent.appendChild(canvas);
        
        function getAsLocation(point) {
            var lat = Math.max(Math.min(85.0511, point[1]), -85.0511),
                lng = Math.max(Math.min(180, point[0]), -180);
            return new MM.Location(lat, lng);
        }
        
        function drawGeometry(ctx, geom) {
            var p = map.locationPoint(geom[0]),
                i;
            ctx.moveTo(p.x, p.y);
            for (i = 1; i < geom.length; i++) {
                p = map.locationPoint(geom[i]);
                ctx.lineTo(p.x, p.y);
            }
            //ctx.closePath();
            //ctx.fill();
            ctx.stroke();
        }
        
        function clear() {
            geometries = [];
            redraw();
        }
        
        function redraw() {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            if (hidden) {
                return;
            }
            ctx.strokeStyle = 'deepskyblue';
            ctx.shadowColor = 'deepskyblue';
            ctx.shadowBlur = 2;
            ctx.lineWidth = 1;
            //ctx.fillStyle = 'transparent';
            ctx.beginPath();
            for (var i = 0; i < geometries.length; i++) {
                drawGeometry(ctx, geometries[i]);
            }
        }
        
        function add(collection) {
            var features = collection.features,
                len = features.length;

            for (var i = 0; i < len; i++) {
                var feature = features[i],
                    geom = [];
                if (feature.type == 'Feature') {
                    var type = feature.geometry.type,
                        coordinates;
                    if (type == 'Polygon') {
                        coordinates = feature.geometry.coordinates[0];
                    }else if (type == 'LineString') {
                        coordinates = feature.geometry.coordinates;
                    }else {
                        continue;
                    }
                    for (var j = 0; j < coordinates.length; j++) {
                        var point = getAsLocation(coordinates[j]);
                        geom.push(point);
                        points.push(point);
                    }
                    geometries.push(geom);
                }
            }
            redraw();
        }
        
        function fitExtent() {
            map.setExtent(points);
            redraw();
        }
        
        map.addCallback('drawn', redraw);
        map.addCallback('resized', function () {
            canvas.width = map.dimensions.x;
            canvas.height = map.dimensions.y;
            redraw();
        });

        function toggle() {
            console.log('toggle geojson overlay poly');
            hidden = !hidden;
            redraw();
        }
        
        return {
            add: add,
            clear: clear,
            fitExtent: fitExtent,
            toggle: toggle
        };
    }
    
    function createMarkerOverlay(map) {
        var markers = new MM.MarkerLayer(),
            extent;
        map.addLayer(markers);
        
        function addMarkers(collection) {
            var features = collection.features,
                len = features.length;
            extent = [];
            for (var i = 0; i < len; i++) {
                var feature = features[i],
                    marker;
                if (feature.geometry.type != 'Point') {
                    continue;
                }
                marker = document.createElement("div");
                marker.setAttribute("class", "gbifBasicMapMarker");
                var img = marker.appendChild(document.createElement("img"));
                img.setAttribute("src", "images/marker-icon.png");

                markers.addMarker(marker, feature);
                extent.push(marker.location);
            }
        }
        
        function fitExtent() {
            map.setExtent(extent);
        }

        function toggle() {
            console.log('Marker toggle is not implemented');
        }
        
        return {
            add: addMarkers,
            fitExtent: fitExtent,
            toggle: toggle
        };
    }
    
    return {
        createPolyOverlay: createPolyOverlay,
        createMarkerOverlay: createMarkerOverlay
    };
})();

},{"modestmaps":1}],18:[function(require,module,exports){
var MM = require('maplib'),
    geoJsonOverlay = require('./geojsonOverlay.js');

module.exports = (function () {
    //MODEST MAPS
    //documentation is a bit scathered
    //https://github.com/modestmaps/modestmaps-js
    //http://code.modestmaps.com/v3.3.4/doc/#Map.getLayers
    //http://modestmaps.com/
    var templates = require('./templates/template.js'),
        map,
        overlay,
        geoJsonOverlays = [],
        mapParent,
        libAttribution = templates.attribution,
        eventlisteners = {extentChanges: []},
        changeTimer;
    function initMap(options) {
        var basemap = options.basemap,
            mapElement,
            baselayer;
        mapParent = options.parentElement;
        mapElement = mapParent.querySelector('.gbifMapComponent_map');
        
        //create map
        baselayer = new MM.TemplatedLayer(basemap.url, basemap.subdomains);
        map = new MM.Map(mapElement, baselayer, null, [
            MM.DragHandler(),
            MM.DoubleClickHandler(),
            MM.MouseWheelHandler(), // MM.MouseWheelHandler(),.precise(true), //for continuous zoom, but seems slow on firefox
            MM.TouchHandler()
        ]);
        
        map.setCenterZoom(new MM.Location(options.lat || 0, options.lng || 0), options.zoom || 1);
        map.setZoomRange(0, 14);
        
        //add handlers
        updateAttribution(basemap.attribution);
        addZoomButtonHandlers();
        addExtentListeners();
        
        
        overlay = new MM.TemplatedLayer('');
        map.insertLayerAt(1, overlay);
        if (options.geoJson) {
            addGeoJson(options.geoJson, typeof options.zoom === 'undefined');
        }
    }
    
    function addZoomButtonHandlers() {
        MM.addEvent(mapParent.querySelector('.gbifMapComponent_zoom-in'), "click", function (e) {
            map.zoomIn();
            return MM.cancelEvent(e);
        });
        
        MM.addEvent(mapParent.querySelector('.gbifMapComponent_zoom-out'), "click", function (e) {
            map.zoomOut();
            return MM.cancelEvent(e);
        });
    }
    
    function addExtentListeners() {
        map.addCallback("panned", mapExtentChange);
        map.addCallback("zoomed", mapExtentChange);
        map.addCallback("resized", mapExtentChange);
        map.addCallback("extentset", mapExtentChange);
    }
    
    function updateAttribution(att) {
        mapParent.querySelector('.attribution>span').innerHTML = libAttribution + att;
    }
    
    function setBaseMap(basemap) {
        var layer = new MM.TemplatedLayer(basemap.url, basemap.subdomains);
        if (!map.getLayerAt(0)) {
            map.insertLayerAt(0, layer);
        }else {
            map.setLayerAt(0, layer);
        }
        updateAttribution(basemap.attribution);
    }

    function setOverlay(templ) {
        overlay = new MM.TemplatedLayer(templ);
        if (!map.getLayerAt(1)) {
            map.insertLayerAt(1, overlay);
        }else {
            map.setLayerAt(1, overlay);
        }
    }
    
    function removeOverlay() {
        if (overlay) {
            overlay.disable();
        }
    }

    function hasOverlay() {
        if (overlay && overlay.enabled) {
            return true;
        }
        return false;
    }
    
    function getExtent() {
        return map.getExtent();
    }
    
    function setExtent(ext) {
        var zoom,
            extent = new MM.Extent(
            new MM.Location(ext.minimumLatitude, ext.minimumLongitude),
            new MM.Location(ext.maximumLatitude, ext.maximumLongitude)
        );
        map.setExtent(extent, false);
        zoom = Math.max(Math.min(map.getZoom(), 6), 1);
        map.setZoom(zoom);
    }
    
    function mapExtentChange(map, change) {
        if (changeTimer) {
            clearTimeout (changeTimer);
        }
        changeTimer = setTimeout(function () {
            notifyListeners(eventlisteners.extentChanges);
        }, 300);
    }

    function notifyListeners(listeners) {
        for (var i = 0; i < listeners.length; i++) {
            listeners[i](map.getExtent());
        }
    }

    function addExtentChangeListener(cb) {
        eventlisteners.extentChanges.push(cb);
    }

    function addGeoJson(collection, zoomToExtent) {
        var features = collection.features,
            len = features.length,
            points, poly;
        
        //contains supported feature types?
        points = features.filter(function (e) {
            return e.geometry.type == 'Point';
        });
        poly = features.filter(function (e) {
            return e.geometry.type == 'Polygon' || e.geometry.type == 'LineString';
        });
        
        //if so add them
        if (poly.length > 0) {
            var polyOverlay = geoJsonOverlay.createPolyOverlay(map);
            geoJsonOverlays.push(polyOverlay);
            polyOverlay.add(collection);
            if (zoomToExtent) {
                polyOverlay.fitExtent();
            }
        }
        if (points.length > 0) {
            var markerOverlay = geoJsonOverlay.createMarkerOverlay(map);
            geoJsonOverlays.push(markerOverlay);
            markerOverlay.add(collection);
            if (zoomToExtent) {
                markerOverlay.fitExtent();
            }
        }
        
    }

    function toggleGeoJsonOverlay() {
        geoJsonOverlays.forEach(function (e) {
            e.toggle();
        });
    }

    return {
        init: initMap,
        setBaseMap: setBaseMap,
        setOverlay: setOverlay,
        removeOverlay: removeOverlay,
        hasOverlay: hasOverlay,
        getExtent: getExtent,
        setExtent: setExtent,
        addExtentChangeListener: addExtentChangeListener,
        addGeoJson: addGeoJson,
        toggleGeoJsonOverlay: toggleGeoJsonOverlay
    };
})();

},{"./geojsonOverlay.js":17,"./templates/template.js":19,"maplib":22}],19:[function(require,module,exports){
var attributionHtml = "<span><a href=\"//www.gbif.org/disclaimer\" target=\"_blank\">Disclaimer</a> <a href=\"http://modestmaps.com/\">Modest maps</a></span>";
module.exports = {
    attribution: attributionHtml
};

},{}],20:[function(require,module,exports){
window.GBIF = window.GBIF || {};
GBIF.basicMap = require('./basicMap.js');

function getURLParameter(name) {
    return decodeURIComponent((new RegExp('[?|&]' + name + '=' + '([^&;]+?)(&|#|;|$)').exec(location.search) || ['', ''])[1].replace(/\+/g, '%20')) || null;
}

/** Parse url and generate a configuration object for the map and interface */
function getQuery() {
    //var route = window.location.hash.toUpperCase().slice(1).split('/'); //for hash routing
    var cat = getURLParameter('cat'),
        simplifyInterface = getURLParameter('simplifyInterface'),
        geojson = getURLParameter('geojson'),
        point = getURLParameter('point');
    
    cat = cat ? cat.split(',') : undefined;
    geojson = geojson ? JSON.parse(geojson) : undefined;
    //if a point is supplied it will override the geojson at this point
    if (point) {
        point = point.split(',');
        simplifyInterface = simplifyInterface === null ? true : simplifyInterface; //when showing a point the default is to simplify the interface
        cat = cat || 'none';
        //geojson expects lon,lat
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "geometry": {"type": "Point", "coordinates": [parseFloat(point[1]), parseFloat(point[0])]}}
            ]
        };
    }

    simplifyInterface = simplifyInterface !== null ? simplifyInterface : false;

    return {
        type: getURLParameter('type'),
        key: getURLParameter('key') || 1,
        resolution: getURLParameter('resolution'),
        lat: getURLParameter('lat'),
        lng: getURLParameter('lng'),
        zoom: getURLParameter('zoom') || getURLParameter('default_zoom'),
        style: getURLParameter('style'),
        geoJson: geojson,
        simplifyInterface: simplifyInterface,
        cat: cat
    };
}
var settings = getQuery();

GBIF.basicMap.createMap(document.body, settings);


// Listen for changes in the map and notify the iframe parent
GBIF.basicMap.setStateListener(function (state) {
    //the post back message is not in the format descirbed in the current documentation,
    //but the one that seems to be used in the current production version.
    parent.postMessage({
        origin: window.name,
        url: window.location.origin,
        searchUrl: state
    }, '*');
});
/*
// listen for messages from the parent (in case of embedded in iframe) and listen for instructions to add geojson layer.
function addGeoJson(evt) {
    if (evt.data.geojson) {
        GBIF.basicMap.addGeoJson(evt.data.geojson);
    }
}

if (window.addEventListener) {
    // For standards-compliant web browsers
    window.addEventListener("message", addGeoJson, false);
} else {
    window.attachEvent("onmessage", addGeoJson);
}
*/

//GBIF.basicMap.addGeoJson({"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,56],[36,56],[36,72],[0,72],[0,56]]]}}]});

//Prevent scroll on body element, used when embeded as iframe and we do not want to scroll parent page ever.
function preventscroll(ev) {
    ev.stopPropagation();
    ev.preventDefault();
    ev.returnValue = false;
    return false;
}

//The joys of browser inconsistencies
var scrollElement = document.querySelector('.gbifBasicMap_mapComponent');

scrollElement.addEventListener('scroll', preventscroll);
scrollElement.addEventListener('mousewheel', preventscroll);
scrollElement.addEventListener('DOMMouseScroll', preventscroll);
scrollElement.addEventListener('MozMousePixelScroll', preventscroll);

},{"./basicMap.js":4}],21:[function(require,module,exports){
var src = "<div id=\"gbifBasicMap\" class=\"gbifBasicMap\" onselectstart=\"return false\">\n    <div class=\"gbifBasicMapWrapper\" rv-class-touch=\"navigation.supportsTouch\" >\n        <div class=\"gbifBasicMap_mapComponent\">\n        \n        <div class=\"gbifMapComponent\">\n            <div class=\"gbifMapComponent_map\"></div>\n            <div class=\"gbifMapComponent_zoom\">\n                <div class=\"button gbifMapComponent_zoom-in\"><i class=\"icon\">+</i></div>\n                <div class=\"button gbifMapComponent_zoom-out\"><i class=\"icon\">-</i></div>\n            </div>\n            <div class=\"attribution\"><div class=\"attribution_title\" rv-text=\"lang.map.attribution\">Attribution, Disclaimer</div> <span></span></div>\n            \n            <div class=\"button toggle filters-toggle\" rv-class-show=\"true\" rv-on-click=\"navigation.showFilters\" rv-on-touchend=\"navigation.showFilters\"><i class=\"gbifBasicMapIcon gbifBasicMapIcon-filter\"></i></div>\n            <div class=\"button toggle styling-toggle\" rv-class-show=\"true\" rv-on-click=\"navigation.showStyling\" rv-on-touchend=\"navigation.showStyling\"><i class=\"gbifBasicMapIcon gbifBasicMapIcon-style\"></i></div>\n            <div rv-unless=\"navigation.hideFullScreenButton\" class=\"button toggle fullscreen\" rv-title=\"lang.navigation.fullscreen\"><i class=\"gbifBasicMapIcon gbifBasicMapIcon-fullscreen\"></i></div>\n        </div>\n\n        </div>\n        <div rv-unless=\"navigation.simplifyInterface\" class=\"gbifBasicMap_nav gbifBasicMap_nav_filters\" rv-class-show=\"navigation.filters\" rv-class-activedates=\"filters.dates.showDates\">\n            <div class=\"gbifBasicMap_sidebarHeader\" rv-on-click=\"navigation.toggleEvidenceMouse\" rv-on-touchend=\"navigation.toggleEvidenceTouch\">\n                <div><i class=\"gbifBasicMapIcon gbifBasicMapIcon-filter\"></i></div>\n                <h2 rv-text=\"lang.filters.headline\"></h2>\n            </div>\n            <ul>\n                <li class=\"gbifBasicMap_nav_evidence\" rv-class-showevidence=\"navigation.evidence\">\n                    <h3 class=\"section\" rv-text=\"lang.filters.evidence.headline\"></h3>\n                    <ul>\n                        <li class=\"undated\" rv-class-active=\"filters.dates.undated.active\">\n                            <div>\n                                <span>\n                                    <span rv-on-click=\"filters.undated.toggleUndatedMouse\"  rv-on-touchend=\"filters.undated.toggleUndatedTouch\" rv-text=\"lang.filters.evidence.includeUndated.text\"></span>\n                                    <i class=\"gbifBasicMapIcon gbifBasicMapIcon-info\" rv-on-click=\"filters.undated.toggleVisibilityMouse\" rv-on-touchend=\"filters.undated.toggleVisibilityTouch\"></i>\n                                </span>\n                                <span class=\"comment gbifBasicMap_description\" rv-class-showdescription=\"filters.undated.showDescription\"  rv-text=\"lang.filters.evidence.includeUndated.description\"></span>\n                            </div>\n                        </li>\n                        <li rv-each-option=\"filters.evidence.options | objectList 'id'\" rv-class-active=\"option.active\">\n                            <div rv-on-click=\"filters.evidence.selectMouse\" rv-on-touchend=\"filters.evidence.selectTouch\">\n                                <span rv-text=\"option.text\"></span>\n                                <i rv-if=\"option.comment\" class=\"comment\" rv-text=\"option.comment\"></i>\n                            </div>\n                        </li>\n                    </ul>\n                </li>\n                <li class=\"gbifBasicMap_nav_dates\" rv-if=\"filters.dates.showDates\">\n                    <h3 class=\"section\" rv-text=\"lang.filters.dates.headline\"></h3>\n                    <ul>\n                        <li rv-each-option=\"filters.dates.options | indexList\" rv-class-active=\"option.active\" rv-class-endpoint=\"option.endpoint\">\n                            <div rv-text=\"option.text\" rv-index=\"option.rv_index\" role=\"button\" rv-on-touchstart=\"filters.dates.touchDragStart\" rv-on-touchmove=\"filters.dates.touchmove\" rv-on-mousedown=\"filters.dates.mouseDragStart\" rv-on-mouseenter=\"filters.dates.mouseenter\">\n                            </div>\n                        </li>\n                    </ul>\n                </li>\n            </ul>\n        </div>\n\n        <div class=\"gbifBasicMap_nav gbifBasicMap_nav_styling\" rv-class-show=\"navigation.styling\">\n            <h2 rv-on-click=\"navigation.hideAll\" rv-text=\"lang.styling.headline\"></h2>\n            <ul>\n                <li class=\"maptype\">\n                    <h3 rv-text=\"lang.styling.basemap.headline\"></h3>\n                    <ul>\n                        <li rv-each-option=\"styling.maps.options | objectList 'id'\" rv-class-active=\"option.active\">\n                            <label>\n                                <input type=\"radio\" rv-value=\"option.id\" rv-checked=\"styling.maps.selectedValue\" name=\"baselayer\" rv-on-click=\"styling.maps.select\">\n                                <div rv-text=\"option.name\"></div>\n                            </label>\n                        </li>\n                    </ul>\n                </li>\n                <li class=\"pointsize\" rv-unless=\"navigation.simplifyInterface\">\n                    <h3 rv-text=\"lang.styling.pointsize.headline\"></h3>\n                    <ul>\n                        <li rv-each-option=\"styling.size.options\" rv-class-active=\"option.active\">\n                            <label>\n                                <input type=\"radio\" rv-value=\"option.resolution\" name=\"resolution\" rv-checked=\"styling.size.selectedValue\" rv-on-click=\"styling.size.select\">\n                                <div rv-text=\"option.name\"></div>\n                            </label>\n                        </li>\n                    </ul>\n                </li>\n                <li class=\"overlay\" rv-if=\"styling.information.show\">\n                    <h3 rv-text=\"lang.styling.overlay.headline\"></h3>\n                    <ul>\n                        <li rv-class-active=\"styling.information.options.geojson.active\">\n                            <div rv-on-click=\"styling.information.options.geojson.click\" rv-on-touchend=\"styling.information.options.geojson.touch\">\n                                <span rv-text=\"lang.styling.overlay.showGeoJson\"></span>\n                            </div>\n                        </li>\n                    </ul>\n                </li>\n            </ul>\n        </div>\n    </div>\n</div>\n";
module.exports = {
    html: src
};

},{}],22:[function(require,module,exports){
var com = {};
com.modestmaps = require('modestmaps');

(function(MM) {

    /**
     * The MarkerLayer doesn't do any tile stuff, so it doesn't need to
     * inherit from MM.Layer. The constructor takes only an optional parent
     * element.
     *
     * Usage:
     *
     * // create the map with some constructor parameters
     * var map = new MM.Map(...);
     * // create a new MarkerLayer instance and add it to the map
     * var layer = new MM.MarkerLayer();
     * map.addLayer(layer);
     * // create a marker element
     * var marker = document.createElement("a");
     * marker.innerHTML = "Stamen";
     * // add it to the layer at the specified geographic location
     * layer.addMarker(marker, new MM.Location(37.764, -122.419));
     * // center the map on the marker's location
     * map.setCenterZoom(marker.location, 13);
     *
     */
    MM.MarkerLayer = function(parent) {
        this.parent = parent || document.createElement('div');
        this.parent.style.cssText = 'position: absolute; top: 0px; left: 0px; width: 100%; height: 100%; margin: 0; padding: 0; z-index: 0';
        this.markers = [];
        this.resetPosition();
    };

    MM.MarkerLayer.prototype = {
        // a list of our markers
        markers: null,
        // the absolute position of the parent element
        position: null,

        // the last coordinate we saw on the map
        lastCoord: null,
        draw: function() {
            // these are our previous and next map center coordinates
            var prev = this.lastCoord,
                next = this.map.pointCoordinate({x: 0, y: 0});
            // if we've recorded the map's previous center...
            if (prev) {
                // if the zoom hasn't changed, find the delta in screen
                // coordinates and pan the parent element
                if (prev.zoom == next.zoom) {
                    var p1 = this.map.coordinatePoint(prev),
                        p2 = this.map.coordinatePoint(next),
                        dx = p1.x - p2.x,
                        dy = p1.y - p2.y;
                    // console.log("panned:", [dx, dy]);
                    this.onPanned(dx, dy);
                // otherwise, reposition all the markers
                } else {
                    this.onZoomed();
                }
            // otherwise, reposition all the markers
            } else {
                this.onZoomed();
            }
            // remember the previous center
            this.lastCoord = next.copy();
        },

        // when zoomed, reset the position and reposition all markers
        onZoomed: function() {
            this.resetPosition();
            this.repositionAllMarkers();
        },

        // when panned, offset the position by the provided screen coordinate x
        // and y values
        onPanned: function(dx, dy) {
            this.position.x += dx;
            this.position.y += dy;
            this.parent.style.left = ~~(this.position.x + .5) + "px";
            this.parent.style.top = ~~(this.position.y + .5) + "px";
        },

        // remove all markers
        removeAllMarkers: function() {
            while (this.markers.length > 0) {
                this.removeMarker(this.markers[0]);
            }
        },

        /**
         * Coerce the provided value into a Location instance. The following
         * values are supported:
         *
         * 1. MM.Location instances
         * 2. Object literals with numeric "lat" and "lon" properties
         * 3. A string in the form "lat,lon"
         * 4. GeoJSON objects with "Point" geometries
         *
         * This function throws an exception on error.
         */
        coerceLocation: function(feature) {
            switch (typeof feature) {
                case "string":
                    // "lat,lon" string
                    return MM.Location.fromString(feature);

                case "object":
                    // GeoJSON
                    if (typeof feature.geometry === "object") {
                        var geom = feature.geometry;
                        switch (geom.type) {
                            // Point geometries => MM.Location
                            case "Point":
                                // coerce the lat and lon values, just in case
                                var lon = Number(geom.coordinates[0]),
                                    lat = Number(geom.coordinates[1]);
                                return new MM.Location(lat, lon);
                        }
                        throw 'Unable to get the location of GeoJSON "' + geom.type + '" geometry!';
                    } else if (feature instanceof MM.Location ||
                        (typeof feature.lat !== "undefined" && typeof feature.lon !== "undefined")) {
                        return feature;
                    } else {
                        throw 'Unknown location object; no "lat" and "lon" properties found!';
                    }
                    break;

                case "undefined":
                    throw 'Location "undefined"';
            }
        },

        /**
         * Add an HTML element as a marker, located at the position of the
         * provided GeoJSON feature, Location instance (or {lat,lon} object
         * literal), or "lat,lon" string.
         */
        addMarker: function(marker, feature) {
            if (!marker || !feature) {
                return null;
            }
            // convert the feature to a Location instance
            marker.location = this.coerceLocation(feature);
            // remember the tile coordinate so we don't have to reproject every time
            marker.coord = this.map.locationCoordinate(marker.location);
            // position: absolute
            marker.style.position = "absolute";
            // update the marker's position
            this.repositionMarker(marker);
            // append it to the DOM
            this.parent.appendChild(marker);
            // add it to the list
            this.markers.push(marker);
            return marker;
        },

        /**
         * Remove the element marker from the layer and the DOM.
         */
        removeMarker: function(marker) {
            var index = this.markers.indexOf(marker);
            if (index > -1) {
                this.markers.splice(index, 1);
            }
            if (marker.parentNode == this.parent) {
                this.parent.removeChild(marker);
            }
            return marker;
        },

        // reset the absolute position of the layer's parent element
        resetPosition: function() {
            this.position = new MM.Point(0, 0);
            this.parent.style.left = this.parent.style.top = "0px";
        },

        // reposition a single marker element
        repositionMarker: function(marker) {
            if (marker.coord) {
                var pos = this.map.coordinatePoint(marker.coord);
                // offset by the layer parent position if x or y is non-zero
                if (this.position.x || this.position.y) {
                    pos.x -= this.position.x;
                    pos.y -= this.position.y;
                }
                marker.style.left = ~~(pos.x + .5) + "px";
                marker.style.top = ~~(pos.y + .5) + "px";
            } else {
                // TODO: throw an error?
            }
        },

        // Reposition al markers
        repositionAllMarkers: function() {
            var len = this.markers.length;
            for (var i = 0; i < len; i++) {
                this.repositionMarker(this.markers[i]);
            }
        }
    };

    // Array.indexOf polyfill courtesy of Mozilla MDN:
    // https://developer.mozilla.org/en/JavaScript/Reference/Global_Objects/Array/indexOf
    if (!Array.prototype.indexOf) {
        Array.prototype.indexOf = function (searchElement /*, fromIndex */ ) {
            "use strict";
            if (this === void 0 || this === null) {
                throw new TypeError();
            }
            var t = Object(this);
            var len = t.length >>> 0;
            if (len === 0) {
                return -1;
            }
            var n = 0;
            if (arguments.length > 0) {
                n = Number(arguments[1]);
                if (n !== n) { // shortcut for verifying if it's NaN
                    n = 0;
                } else if (n !== 0 && n !== Infinity && n !== -Infinity) {
                    n = (n > 0 || -1) * Math.floor(Math.abs(n));
                }
            }
            if (n >= len) {
                return -1;
            }
            var k = n >= 0 ? n : Math.max(len - Math.abs(n), 0);
            for (; k < len; k++) {
                if (k in t && t[k] === searchElement) {
                    return k;
                }
            }
            return -1;
        }
    }

})(com.modestmaps);


module.exports = com.modestmaps;

},{"modestmaps":1}],23:[function(require,module,exports){
var sightglass = require('sightglass'),
    rivets = require('rivets');

/**
add an rv_index property to the list, starting at the index specified (defaults to 0)
*/
rivets.formatters.indexList = function (obj, startAtIndex) {
    return (function () {
        if (!startAtIndex) {
            startAtIndex = 0;
        }
        return obj.slice(startAtIndex).map(function (e, index) {
            e.rv_index = index + startAtIndex;
            return e;
        });
    })();
};

/**
Example usage:
rv-each-option="filters.evidence.options | objectList 'id'"
Will turn the object into a list adding the key as 'id'
So {a: {test: 5}} will become [{test: 5, id: a}]
*/
rivets.formatters.objectList = function (obj, id) {
    return (function () {
        var list = [],
            element, key;
        for (key in obj) {
            if (obj.hasOwnProperty(key)) {
                element = obj[key];
                element[id] = key;
                list.push(element);
            }
        }
        return list;
    })();
};

/**
Set the attribute index to the specified value
Example
rv-index="option.myindexvalue"
*/
rivets.binders.index = function (el, value) {
    el.setAttribute('index', value);
};

module.exports = rivets;
},{"rivets":2,"sightglass":3}]},{},[20]);
