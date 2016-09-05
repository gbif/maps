'use strict';

/**
 * Given a destination object and optionally many source objects,
 * copy all properties from the source objects into the destination.
 * The last source object given overrides properties from previous
 * source objects.
 * @param {Object} dest destination object
 * @param {...Object} sources sources from which properties are pulled
 * @returns {Object} dest
 * @private
 */
exports.extend = function (dest) {
  for (var i = 1; i < arguments.length; i++) {
    var src = arguments[i];
    for (var k in src) {
      dest[k] = src[k];
    }
  }
  return dest;
};

exports.forEach = function (array, callback, scope) {
  for (var i = 0; i < array.length; i++) {
    callback.call(scope, i, array[i]);
  }
};
