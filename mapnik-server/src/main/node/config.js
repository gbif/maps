"use strict";

const fs = require('fs')
    , yaml = require('yaml-js');

var configFile = process.argv[2];
console.log("Using config: " + configFile);
var config = yaml.load(fs.readFileSync(configFile, "utf8"));

module.exports = config;
