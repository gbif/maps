package org.gbif.maps.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This is the driver for backfilling HBase maps.
  * A typical invocation during development would look like this:
  * <pre>
  * ~/spark/bin/spark-submit --master yarn --jars $HIVE_CLASSPATH --num-executors 7 --executor-memory 6g --executor-cores 5 --deploy-mode client --class "org.gbif.maps.spark.Backfill" --driver-class-path . spark-process-0.1-SNAPSHOT.jar all dev.yml
  * </pre>
  */
object Backfill {
  val usage = "Usage: [all,tiles,points] configFile"

  /**
    * The main entry point to backfilling.
    * @param args Expects 2 args: [all,tiles,points] configFile
    */
  def main(args: Array[String]) = {
    checkArgs(args) // sanitize input

    // load application config
    val config: MapConfiguration = Configurations.fromFile(args(1))

    // setup and read the source
    val conf = new SparkConf().setAppName(config.appName)
    conf.setIfMissing("spark.master", "local[2]") // 2 threads for local dev in IDE, ignored when run on a cluster
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(config.sourceFile)

    // get a count of records per mapKey
    val counts = df.flatMap(MapUtils.mapKeysForRecord(_)).countByValue()

    if (Set("all","points").contains(args(0))) {
      // upto the threshold we can store points
      val mapKeys = counts.filter(r => {r._2<config.tilesThreshold})
      println("MapKeys suitable for storing as point maps: " + mapKeys.size)
      BackfillPoints.build(sc,df,mapKeys.keySet,config)
    }

    if (Set("all","tiles").contains(args(0))) {
      // above the threshold we build a pyramid
      val mapKeys = counts.filter(r => {r._2>=config.tilesThreshold})
      println("MapKeys suitable for creating tile pyramid maps: " + mapKeys.size)
      // TODO: build tiles
    }

  }

  /**
    * Sanitizes application arguments.
    */
  private def checkArgs(args: Array[String]) = {
    assert(args !=null && args.length==2, usage)
    assert(args(0).equals("all") || args(0).equals("tiles") || args(0).equals("points"), usage)
  }
}

