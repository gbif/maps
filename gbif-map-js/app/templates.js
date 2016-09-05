/**
 * This file exists only to provide the mapping for the build time html2json gulp task.
 *
 * Once the nunjucks has rendered the HTML into the fragments folder, the html-to-json will execute reading this
 * file to produce a simple JSON file whereby each HTML fragment is available as a string keyed on the name.
 *
 * By doing this, we are able to require(.) the JSON which represents the framgents as Strings with no runtime
 * additional dependencies, which is a design goal.
 *
 * Notes:
 *   1. We simply use the * naming convention, so that the name of the file results as the key in the JSON.
 *   2. html-to-json only works on files with an .html extension
 *   3. We include all fragments, named by the filename
 */
//= * : ./*.html
