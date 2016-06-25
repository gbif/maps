package org.gbif.maps.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This is the driver for backfilling HBase maps.
  */
object Backfill {
  val usage = "Usage: [all,tiles,points] configFile"

  /**
    * Reads the dataframe and returns a map of mapKey:recordCount
    */
  def countsByMapKey(df : DataFrame): scala.collection.Map[String,Long] = {
    df.flatMap(MapUtils.mapKeysForRecord(_)).countByValue()

    /*
    df.flatMap(row => {
      val mapKeys = MapUtils.mapKeysForRecord(row)
      mapKeys.foreach(key => {(key,1)})
      // TODO

      val datasetKey = row.getString(row.fieldIndex("datasetkey"))
      val publisherKey = row.getString(row.fieldIndex("publishingorgkey"))
      val country = row.getString(row.fieldIndex("countrycode"))
      val publishingCountry = row.getString(row.fieldIndex("publishingcountry"))

      var taxonIDs = Set[Int]()
      if (!row.isNullAt(row.fieldIndex("kingdomkey"))) taxonIDs+=row.getInt(row.fieldIndex("kingdomkey"))
      if (!row.isNullAt(row.fieldIndex("phylumkey"))) taxonIDs+=row.getInt(row.fieldIndex("phylumkey"))
      if (!row.isNullAt(row.fieldIndex("classkey"))) taxonIDs+=row.getInt(row.fieldIndex("classkey"))
      if (!row.isNullAt(row.fieldIndex("orderkey"))) taxonIDs+=row.getInt(row.fieldIndex("orderkey"))
      if (!row.isNullAt(row.fieldIndex("familykey"))) taxonIDs+=row.getInt(row.fieldIndex("familykey"))
      if (!row.isNullAt(row.fieldIndex("genuskey"))) taxonIDs+=row.getInt(row.fieldIndex("genuskey"))
      if (!row.isNullAt(row.fieldIndex("specieskey"))) taxonIDs+=row.getInt(row.fieldIndex("specieskey"))
      if (!row.isNullAt(row.fieldIndex("taxonkey"))) taxonIDs+=row.getInt(row.fieldIndex("taxonkey"))

      val res = mutable.ArrayBuffer(
        (toMapKey(MAPS_TYPES("DATASET"), datasetKey), 1),
        (toMapKey(MAPS_TYPES("PUBLISHER"), publisherKey), 1),
        (toMapKey(MAPS_TYPES("DATASET"), country), 1),
        (toMapKey(MAPS_TYPES("PUBLISHING_COUNTRY"), publishingCountry), 1)
      )
      taxonIDs.foreach(id => {
        res += ((toMapKey(MAPS_TYPES("TAXON"), id), 1))
      })
      res
    }).countByKey()
    */

  }

  def main(args: Array[String]): Unit = {
    checkArgs(args) // sanitize input

    // load application config
    val config: MapConfiguration = Configurations.fromFile(args(1))

    // setup and read the source
    val conf = new SparkConf().setAppName(config.appName)
    conf.setIfMissing("spark.master", "local[2]") // 2 threads for local dev, ignored when run on cluster
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(config.sourceFile)

    // get a count of records per mapKey
    val counts = countsByMapKey(df)

    if (Set("all","points").contains(args(0))) {
      // upto the threshold we can store points
      val mapKeys = counts.filter(r => {r._2<config.tilesThreshold})
      println("MapKeys suitable for point maps: " + mapKeys.size)
      // TODO: build points
    }

    if (Set("all","tiles").contains(args(0))) {
      // above the threshold we build a pyramid
      val mapKeys = counts.filter(r => {r._2>=config.tilesThreshold})
      println("MapKeys suitable for tile pyramid maps: " + mapKeys.size)
      // TODO: build tiles
    }

  }

  /**
    * Sanitize application arguments.
    */
  private def checkArgs(args: Array[String]) = {
    assert(args !=null && args.length==2, usage)
    assert(args(0).equals("all") || args(0).equals("tiles") || args(0).equals("points"), usage)
  }
}

