package org.gbif.maps.spark

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.base.Preconditions
import com.google.common.io.Resources
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, PatchedHFileOutputFormat2}
import org.apache.hadoop.mapreduce.Job
import org.gbif.maps.common.projection.TileSchema

/**
  * Utility builders
  */
object Configurations {

  /**
    * Returns the application cofiguration for the given file.
    * @param file The YAML config file on the classpath
    * @return The application configuration
    */
  def fromFile(file : String) : MapConfiguration = {
    val confUrl = Resources.getResource(file)
    val mapper = new ObjectMapper(new YAMLFactory())
    val config: MapConfiguration = mapper.readValue(confUrl, classOf[MapConfiguration])
    config
  }

  /**
    * Returns a configured Hadoop job configuration suitable for writing HFiles using the table and cluster defined in the
    * application configuration.
    * @param appConfig Which defines the HBase parameters
    * @param tableName To write to
    * @return A populated Hadoop configuration suitable for writing HFiles
    */
  def hfileOutputConfiguration(appConfig: MapConfiguration, tableName: String) : Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", appConfig.hbase.zkQuorum);

    // NOTE: job creates a copy of the conf
    val job = new Job(conf, appConfig.appName) // name not actually used since we don't submit MR
    val table = new HTable(conf, tableName)
    PatchedHFileOutputFormat2.configureIncrementalLoad(job, table);

    return job.getConfiguration // important
  }
}

/**
  * The configuration for the backfill tile jobs.
  */
class MapConfiguration (
  @JsonProperty("appName") _appName: String,
  @JsonProperty("source") _source: String,
  @JsonProperty("targetDirectory") _targetDirectory: String,
  @JsonProperty("tilesThreshold") _tilesThreshold: Int,
  @JsonProperty("hbase") _hbase: HBaseConfiguration,
  @JsonProperty("pointFeatures") _pointFeatures: PointFeaturesConfiguration,
  @JsonProperty("tilePyramid") _tilePyramid: TilePyramidConfiguration
) extends Serializable {
  val appName = Preconditions.checkNotNull(_appName, "appName cannot be null" : Object)
  var source:String = Preconditions.checkNotNull(_source, "source cannot be null" : Object)
  var targetDirectory = Preconditions.checkNotNull(_targetDirectory, "targetDirectory cannot be null" : Object)
  val tilesThreshold = Preconditions.checkNotNull(_tilesThreshold, "tilesThreshold cannot be null" : Object)
  val hbase = Preconditions.checkNotNull(_hbase, "hbase cannot be null" : Object)
  val pointFeatures = Preconditions.checkNotNull(_pointFeatures, "pointFeatures cannot be null" : Object)
  val tilePyramid = Preconditions.checkNotNull(_tilePyramid, "tilePyramid cannot be null" : Object)
}

/**
  * Configuraiton specific to the tile pyramiding.
  */
class PointFeaturesConfiguration (
  @JsonProperty("numTasks") _numTasks: Int,
  @JsonProperty("tableName") _tableName: String,
  @JsonProperty("hfileCount") _hfileCount: Int
) extends Serializable {
  val numTasks = Preconditions.checkNotNull(_numTasks, "pointNumTasks cannot be null" : Object)
  var tableName = Preconditions.checkNotNull(_tableName, "tableName cannot be null" : Object)
  val hfileCount = Preconditions.checkNotNull(_hfileCount, "hfileCount cannot be null" : Object)
}

/**
  * Configuration specific to the tile pyramiding.
  */
class TilePyramidConfiguration (
  @JsonProperty("tableName") _tableName: String,
  @JsonProperty("hfileCount") _hfileCount: Int,
  @JsonProperty("projections") _projections: Array[ProjectionConfig],
  @JsonProperty("numPartitions") _numPartitions: Int,
  @JsonProperty("tileBufferSize") _tileBufferSize: Int
) extends Serializable {
  var tableName = Preconditions.checkNotNull(_tableName, "tableName cannot be null" : Object)
  val hfileCount = Preconditions.checkNotNull(_hfileCount, "hfileCount cannot be null" : Object)
  val projections = Preconditions.checkNotNull(_projections, "projections cannot be null" : Object)
  val numPartitions = Preconditions.checkNotNull(_numPartitions, "numPartitions cannot be null" : Object)
  val tileBufferSize = Preconditions.checkNotNull(_tileBufferSize, "tileBufferSize cannot be null" : Object)
}

/**
  * Configuration specific to a project used in a tile pyramid.
  */
class ProjectionConfig  (
  @JsonProperty("tileSize") _tileSize: Int,
  @JsonProperty("minZoom") _minZoom: Int,
  @JsonProperty("maxZoom") _maxZoom: Int,
  @JsonProperty("srs") _srs: String
) extends Serializable {
  val tileSize = Preconditions.checkNotNull(_tileSize, "tileSize cannot be null" : Object)
  val minZoom = Preconditions.checkNotNull(_minZoom, "minZoom cannot be null" : Object)
  val maxZoom = Preconditions.checkNotNull(_maxZoom, "maxZoom cannot be null" : Object)
  val srs = Preconditions.checkNotNull(_srs, "maxZoom cannot be null" : Object)
  val tileSchema = TileSchema.fromSRS(srs)
}

/**
  * Configuration specific to the HBase.
  */
class HBaseConfiguration (
  @JsonProperty("zkQuorum") _zkQuorum: String,
  @JsonProperty("rootDir") _rootDir: String,
  @JsonProperty("keySaltModulus") _keySaltModulus: Int
) extends Serializable {
  val rootDir = Preconditions.checkNotNull(_rootDir, "rootDir cannot be null" : Object)
  var zkQuorum = Preconditions.checkNotNull(_zkQuorum, "zkQuorum cannot be null" : Object)
  val keySaltModulus = Preconditions.checkNotNull(_keySaltModulus, "keySaltModulus cannot be null" : Object)
}
