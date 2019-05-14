"use strict";

const quickLocalIp = require('quick-local-ip')
    , uuid = require('uuid')
    , ZooKeeper = require ("zookeeper");

var gbifServiceRegistry = {};

var serviceName = "es-mapnik-server";

/**
 * Register the service in ZooKeeper, with data sufficiently close to what
 * our Java gbif-microservice provides.
 */

var zk;
gbifServiceRegistry.register = function(config) {
  if (!(config.service && config.service.zkHost)) {
    console.log("ZooKeeper not configured");
    return;
  }

  console.log("Registering with ZooKeeper " + config.service.zkHost);

  zk = new ZooKeeper({
    connect: config.service.zkHost,
    timeout: 20000,
    debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN
  });
  zk.connect(function (err) {
    if (err) throw err;
    console.log ("ZooKeeper session established, id=%s", zk.client_id);

    var zkPath = "/"+config.service.zkPath+"/"+serviceName;

    // Create ZK parent node
    zk.a_create (zkPath, "", null, () => {});

    // Create ZK instance node
    var zkUuid = uuid.v4();

    var host = config.service.host;
    var port = parseInt(process.argv[3]);
    var ip = quickLocalIp.getLocalIP4();
    var url = "http://"+host+":"+port+"/";

    var zkValue =
      {
        "name": serviceName,
        "id": zkUuid,
        "address": ip,
        "port": port,
        "sslPort": null,
        "payload": {
          "@class": "org.gbif.ws.discovery.conf.ServiceDetails",
          "groupId": "org.gbif.maps",
          "artifactId": serviceName,
          "version": config.service.version,
          "serviceConfiguration": {
            "httpPort": port,
            "httpAdminPort": -1,
            "zkHost": config.service.zkHost,
            "zkPath": config.service.zkPath,
            "stopSecret": null,
            "timestamp": config.service.timestamp,
            "externalPort": port,
            "externalAdminPort": -1,
            "host": host,
            "containerName": null,
            "conf": process.argv[2],
            "runsInContainer": false,
            "discoverable": true
          },
          "status": "RUNNING",
          "name": serviceName,
          "externalUrl": url,
          "fullName": serviceName + "-" + config.service.version
        },
        "registrationTimeUTC": Date.now(),
        "serviceType": "DYNAMIC",
        "uriSpec": {
          "parts": [
            {
              "value": url,
              "variable": false
            }
          ]
        }
      };

    zk.a_create (zkPath+"/"+zkUuid, JSON.stringify(zkValue), ZooKeeper.ZOO_EPHEMERAL, function (rc, error, path)  {
      if (rc != 0) {
        console.log ("ZooKeeper node create result: %d, error: '%s', path=%s", rc, error, path);
      } else {
        console.log ("Created ZooKeeper node %s", path);
      }
    });
  });

  process.on('SIGINT', gbifServiceRegistry.unregister.bind());
  process.on('SIGTERM', gbifServiceRegistry.unregister.bind());
  process.on('exit', gbifServiceRegistry.unregister.bind());
}

gbifServiceRegistry.unregister = function() {
  if (zk) {
    console.log("Unregistering from ZooKeeper");
    zk.close();
  } else {
    console.log("Was not registered in ZooKeeper");
  }
}

module.exports = gbifServiceRegistry;
