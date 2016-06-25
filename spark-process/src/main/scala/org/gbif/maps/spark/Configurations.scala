package org.gbif.maps.spark

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.base.Preconditions
import com.google.common.io.Resources

/**
  * Utility builder
  */
object Configurations {
  def fromFile(file : String) : MapConfiguration = {
    val confUrl = Resources.getResource(file)
    val mapper = new ObjectMapper(new YAMLFactory())
    val config: MapConfiguration = mapper.readValue(confUrl, classOf[MapConfiguration])
    config
  }
}

/**
  * The configuration for the backfill tile jobs.
  */
class MapConfiguration (
  @JsonProperty("appName") _appName: String,
  @JsonProperty("sourceFile") _sourceFile: String,
  @JsonProperty("targetDirectory") _targetDirectory: String,
  @JsonProperty("tilesThreshold") _tilesThreshold: Int,
  @JsonProperty("hbase") _hbase: HBaseConfiguration,
  @JsonProperty("tilePyramid") _tilePyramid: TilePyramidConfiguration
) {
  val appName = Preconditions.checkNotNull(_appName, "appName cannot be null" : Object)
  val sourceFile = Preconditions.checkNotNull(_sourceFile, "sourceFile cannot be null" : Object)
  val targetDirectory = Preconditions.checkNotNull(_targetDirectory, "targetDirectory cannot be null" : Object)
  val tilesThreshold = Preconditions.checkNotNull(_tilesThreshold, "tilesThreshold cannot be null" : Object)
  val hbase = Preconditions.checkNotNull(_hbase, "hbase cannot be null" : Object)
  val tilePyramid = Preconditions.checkNotNull(_tilePyramid, "tilePyramid cannot be null" : Object)
}

/**
  * Configuraiton specific to the tile pyramiding.
  */
class TilePyramidConfiguration (
  @JsonProperty("tileSize") _tileSize: Int,
  @JsonProperty("minZoom") _minZoom: Int,
  @JsonProperty("maxZoom") _maxZoom: Int,
  @JsonProperty("srs") _srs: String) {

  val tileSize = Preconditions.checkNotNull(_tileSize, "tileSize cannot be null" : Object)
  val minZoom = Preconditions.checkNotNull(_minZoom, "minZoom cannot be null" : Object)
  val maxZoom = Preconditions.checkNotNull(_maxZoom, "maxZoom cannot be null" : Object)
  val srs = Preconditions.checkNotNull(_srs, "maxZoom cannot be null" : Object).split(",")
}


/**
  * Configuration specific to the HBase.
  */
class HBaseConfiguration (
  @JsonProperty("zkQuorum") _zkQuorum: String,
  @JsonProperty("tableName") _tableName: String,
  @JsonProperty("hfileCount") _hfileCount: String) {
  val zkQuorum = Preconditions.checkNotNull(_zkQuorum, "zkQuorum cannot be null" : Object)
  val tableName = Preconditions.checkNotNull(_tableName, "tableName cannot be null" : Object)
  val hfileCount = Preconditions.checkNotNull(_hfileCount, "hfileCount cannot be null" : Object)
}
