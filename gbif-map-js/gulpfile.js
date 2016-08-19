var gulp = require('gulp');
var source = require('vinyl-source-stream');
var buffer = require('vinyl-buffer');
var browserify = require('browserify');
var notify = require('gulp-notify');
var fs = require('fs');
var del = require('del');
var nunjucksRender = require('gulp-nunjucks-render');
var data = require('gulp-data');
var htmltojson = require('gulp-html-to-json');
var rename = require("gulp-rename");
var uglify = require("gulp-uglify");
var inject = require('gulp-inject-string');

// The locales that will be built
var locales = ['en'];

// TODO: add a version number to the project, which will be included in the final artifact js name

// Build the world by default
gulp.task('default', locales.map(function(locale) {
  return 'scripts-' + locale;
}));

/**
 * Run a separate build for each locale.
 * The build process involces creating a scratch folder for the locale, generating HTML fragments using the templated
 * files plus the locale string, converting those to JSON Strings for easy inclusion, and then a standard browserify
 * build to produce a locale specific JS file.
 */
locales.forEach(function(locale) {
  // cleanup
  gulp.task('clean-'+locale, function() {
    return del('.scratch/' + locale);
  });

  // render the nunjucks templates
  gulp.task('render-template-'+locale, ['clean-' + locale], function() {

    return gulp.src('app/*.tmpl')
      // parse the locale data
      .pipe(data(JSON.parse(fs.readFileSync('app/' + locale + ".json", 'utf8'))))
      .pipe(nunjucksRender())
      .pipe(gulp.dest('.scratch/' + locale + '/fragments'))
  });

  // sets up the html-to-json stage (requires a JS to dictate what runs)
  gulp.task('setup-html-to-json-'+locale, ['render-template-'+locale], function() {
    return gulp.src('app/templates.js')
      .pipe(gulp.dest('.scratch/' + locale + '/fragments'))
  });

  // converts the HTML fragments to JSON
  gulp.task('html-to-json-'+locale, ['setup-html-to-json-'+locale], function() {
    return gulp.src('.scratch/' + locale + '/fragments/templates.js')
      .pipe(htmltojson({
        useAsVariable: false // will result in a template.json
      }))
      .pipe(gulp.dest('.scratch/' + locale))
  });

  // prepare the scripts by injecting the templated JSON
  gulp.task('setup-scripts-'+locale, ['html-to-json-'+locale], function() {
    return gulp.src('app/gbif-map.js')
      .pipe(inject.prepend('var fragments = require("./templates.json");'))
      .pipe(gulp.dest('.scratch/' + locale))
  });

  // run the build
  // todo: separate debug versions, add source maps, add version number to final build name
  gulp.task('scripts-'+locale, ['setup-scripts-'+locale], function() {
    var src = '.scratch/' + locale + '/gbif-map.js';
    return browserify(src, {standalone: 'gbif'})
      .bundle()
      .on("error", notify.onError(function (error) {
        return error.message;
      }))
      .pipe(source('gbif-map.js'))
      .pipe(buffer())
      .pipe(uglify())
      .pipe(rename("gbif-map_"+ locale +".min.js"))
      .pipe(gulp.dest('./dist'));
  });
})

