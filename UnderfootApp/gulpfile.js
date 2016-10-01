var gulp = require('gulp'),
    gulpWatch = require('gulp-watch'),
    del = require('del'),
    runSequence = require('run-sequence'),
    argv = process.argv;

/**
 * Ionic hooks
 * Add ':before' or ':after' to any Ionic project command name to run the specified
 * tasks before or after the command.
 */
gulp.task('serve:before', ['watch']);
gulp.task('emulate:before', ['build']);
gulp.task('deploy:before', ['build']);
gulp.task('build:before', ['build']);

// we want to 'watch' when livereloading
var shouldWatch = argv.indexOf('-l') > -1 || argv.indexOf('--livereload') > -1;
gulp.task('run:before', [shouldWatch ? 'watch' : 'build']);

/**
 * Ionic Gulp tasks, for more information on each see
 * https://github.com/driftyco/ionic-gulp-tasks
 *
 * Using these will allow you to stay up to date if the default Ionic 2 build
 * changes, but you are of course welcome (and encouraged) to customize your
 * build however you see fit.
 */
var buildBrowserify = require('ionic-gulp-browserify-typescript');
var buildSass = require('ionic-gulp-sass-build');
var copyHTML = require('ionic-gulp-html-copy');
var copyFonts = require('ionic-gulp-fonts-copy');
var copyScripts = require('ionic-gulp-scripts-copy');
var tslint = require('ionic-gulp-tslint');

var isRelease = argv.indexOf('--release') > -1;

gulp.task('watch', ['clean'], function(done){
  runSequence(
    // ['sass', 'html', 'fonts', 'scripts'],
    ['sass', 'html', 'fonts', 'scripts', 'extlibs'],
    function(){
      gulpWatch('app/**/*.scss', function(){ gulp.start('sass'); });
      gulpWatch('app/**/*.html', function(){ gulp.start('html'); });
      buildBrowserify({ watch: true }).on('end', done);
    }
  );
});

gulp.task('build', ['clean'], function(done){
  runSequence(
    // ['sass', 'html', 'fonts', 'scripts'],
    ['sass', 'html', 'fonts', 'scripts', 'extlibs'],
    function(){
      buildBrowserify({
        minify: isRelease,
        browserifyOptions: {
          debug: !isRelease
        },
        uglifyOptions: {
          mangle: false
        }
      }).on('end', done);
    }
  );
});

// https://www.thepolyglotdeveloper.com/2016/06/alter-ionic-2-gulp-script-include-browser-javascript-files/
//
// Build a single JS file from external dependencies in www/extlibs/. This
// also requires some other changes to this gulpfile (see link and/or refs to
// extlibs here), and an additional line in www/index.html. Note that for
// leaflet and presumably anything else with CSS deps, I've put those files in
// app/theme/extlibs, renamed them to end in .scss, and imported them where
// appropriate in other SASS stylesheets.
//
// Also note that I'm using https://www.npmjs.com/package/typings to ensure
// that the typescript transpiler doesn't freak out about all the untyped
// stuff in leaflet, e.g.
//
// typings install dt~mocha --global --save
//
// That's using a pre-made type mapping that's probably based on an older
// version of leaflet, but it seems to work.
var gulpConcat = require('gulp-concat');
gulp.task('extlibs', function() {
  return gulp.src('www/extlibs/*.js')
    .pipe(gulpConcat('external-libraries.js'))
    .pipe(gulp.dest('www/build/js'));
});

gulp.task('sass', buildSass);
// gulp.task('sass', function( ) {
//   return buildSass({
//     sassOptions: {
//       includePaths: [
//         'node_modules/ionic-angular',
//         'node_modules/ionicons/dist/scss',
//         'node_modules/leaflet/dist'
//       ]
//     }
//   })
// })
gulp.task('html', copyHTML);
gulp.task('fonts', copyFonts);
gulp.task('scripts', copyScripts);
// gulp.task('scripts', function() {
//   return copyScripts({
//     src: [
//       'node_modules/es6-shim/es6-shim.min.js',
//       'node_modules/es6-shim/es6-shim.map',
//       'node_modules/zone.js/dist/zone.js',
//       'node_modules/reflect-metadata/Reflect.js',
//       'node_modules/reflect-metadata/Reflect.js.map',
//       'node_modules/leaflet/dist/leaflet.js'
//     ]
//   })
// })
gulp.task('clean', function(){
  return del('www/build');
});
gulp.task('lint', tslint);
