package org.gbif.maps.spark

import scala.collection.JavaConverters._

import java.util.concurrent.{Callable, ExecutorService, Executors}

import org.apache.spark.{SparkConf, SparkContext}
import org.gbif.maps.workflow.WorkflowParams
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * This is the driver for backfilling HBase maps.
  * A typical invocation during development would look like this:
  * <pre>
  * ~/spark/bin/spark-submit --master yarn --jars $HIVE_CLASSPATH --num-executors 7 --executor-memory 6g --executor-cores 5 --deploy-mode client --class "org.gbif.maps.spark.Backfill" --driver-class-path . spark-process-0.1-SNAPSHOT.jar all dev.yml
  * </pre>
  */
object Backfill {
  val logger = LoggerFactory.getLogger("org.gbif.maps.spark.Backfill")

  val usage = "Usage: all|tiles|points configFile [optionalSource]"

  /**
    * The main entry point to backfilling.
    * @param args Expects 2 args: [all,tiles,points] configFile
    */
  def main(args: Array[String]) = {
    checkArgs(args) // sanitize input

    // load application config
    val config: MapConfiguration = Configurations.fromFile(args(1))

    // The following exists only to allow usage in the Oozie workflow.
    // Oozie does not support templated files, and therefore we opt to override parameters that are calculated at
    // runtime in the Oozie workflow.  These simply overwrite whatever is supplied in the YAML configuration file
    // in args(1).  Note: this was a considered decision, opting to a) keep the spark module nice to develop with a
    // single config and b) using YAML to keep the array functionality which is a nuisance in the config formats
    // possible in Oozie.
    if (args.length == 3) {
      logger.warn("Overwriting config with Oozie supplied configuration")
      var overrideParams = WorkflowParams.buildFromOozie(args(2))
      config.hbase.zkQuorum = overrideParams.getZkQuorum
      config.source = overrideParams.getSnapshotTable
      config.pointFeatures.tableName = overrideParams.getTargetTable
      config.tilePyramid.tableName = overrideParams.getTargetTable
      config.targetDirectory = overrideParams.getTargetDirectory
    }

    // setup and read the source
    val conf = new SparkConf().setAppName(config.appName)
    conf.setIfMissing("spark.master", "local[2]") // 2 threads for local dev in IDE, ignored when run on a cluster
    val sc = new SparkContext(conf)

    val df =
      if (config.source.startsWith("/")) {
        logger.info("Reading Parquet file {}", config.source)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        sqlContext.read.parquet(config.source)
      }
      else {
        logger.info("Reading from HBase snapshot table {}", config.source)
        HBaseInput.readFromHBase(config, sc)
      }

    logger.info("DataFrame columns are {}", df.columns)

    if (Set("all","points").contains(args(0))) {
      BackfillPoints.build(sc,df,config)
    }

    if (Set("all","tiles").contains(args(0))) {
      // get a count of records per mapKey
      val counts = df.flatMap(MapUtils.mapKeysForRecord(_)).countByValue()

      // above the threshold we build a pyramid
      val mapKeys = counts.filter(r => {r._2>=config.tilesThreshold})
      println("MapKeys suitable for creating tile pyramid maps: " + mapKeys.size)

      //val pool = Executors.newFixedThreadPool(config.tilePyramid.projections.length)
      //val jobs : ListBuffer[Callable[Unit]] = new ListBuffer[Callable[Unit]]()



      config.tilePyramid.projections.foreach(proj => {
        println("Building tiles for projection" + proj.srs)

        //jobs += new Callable[Unit] {
        //  override def call() = BackfillTiles.build(sc,df,mapKeys.keySet,config, proj)
        //}
        BackfillTiles.build(sc,df,mapKeys.keySet,config, proj)
      })

      //pool.invokeAll(jobs.toList.asJava)


    }

    sc.stop()
  }

  /**
    * Sanitizes application arguments.
    */
  private def checkArgs(args: Array[String]) = {
    assert(args !=null && (args.length==2 || args.length==3), usage)
    assert(args(0).equals("all") || args(0).equals("tiles") || args(0).equals("points"), usage)
  }
}
