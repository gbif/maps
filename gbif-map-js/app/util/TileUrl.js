'use strict';

/**
 * Appends a new element of type tagName with a className to the container.
 */
module.exports = {
  TileURL: function () {
    var baseUrl;
    var srs;

    var url = function() {
      var params = [];

      if (srs !== undefined && srs) {
        params.push("srs=" + srs);
      }

      if (params.length == 0) {
        return baseUrl;
      } else {
        return baseUrl + '?' + params.join("&");
      }
    }

  }
}
