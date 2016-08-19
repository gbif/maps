'use strict';

/**
 * Appends a new element of type tagName with a className to the container.
 */
exports.create = function (tagName, className, container) {
  var el = document.createElement(tagName);
  if (className) el.className = className;
  if (container) container.appendChild(el);
  return el;
};
