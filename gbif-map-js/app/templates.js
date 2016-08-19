/**
 * This file provides the mapping for the build time html2json gulp task.
 * Once the nunjucks has rendered the HTML into the fragments folder, the html-to-json will execute reading this
 * file to produce a simple JSON file whereby each HTML fragment is available as a string keyed on the name.
 * Notes:
 * 1. We simply use the * naming convention, so that the name of the file results as the key in the JSON.
 * 2. html-to-json only works on files with an .html extension
 */
//= * : ./*.html
