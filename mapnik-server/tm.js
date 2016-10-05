/**
 * Contains only those methods required for the carto.js and cherry picked from
 *   https://github.com/mapbox/mapbox-studio-classic/blob/mb-pages/lib/tm.js
 */
var tm = {};

// Return an object with sorted keys, ignoring case.
// Note: We don't use this, as it results in infinite loop since the exception is not thrown, but we leave the
// method in case we need this again.  See the end of carto.js for it's only use.
tm.sortkeys = function(obj) {
  try {
    return obj.map(tm.sortkeys);
  } catch(e) {};
  try {
    return Object.keys(obj).sort(function(a, b) {
      a = a.toLowerCase();
      b = b.toLowerCase();
      if (a === 'id') return -1;
      if (b === 'id') return 1;
      if (a > b) return 1;
      if (a < b) return -1;
      return 0;
    }).reduce(function(memo, key) {
      memo[key] = tm.sortkeys(obj[key]);
      return memo;
    }, {});
  } catch(e) { return obj };
};

// Named projections.
tm.srs = {
  'WGS84': '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
  '900913': '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0.0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over'
};
tm.extent = {
  'WGS84': '-180,-85.0511,180,85.0511',
  '900913': '-20037508.34,-20037508.34,20037508.34,20037508.34'
};
// Reverse the above hash to allow for srs name lookups.
tm.srsname = {};
for (name in tm.srs) tm.srsname[tm.srs[name]] = name;

module.exports = tm;
