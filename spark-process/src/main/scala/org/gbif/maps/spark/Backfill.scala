package org.gbif.maps.spark

import java.util.{Properties, UUID}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import com.rojoma.simplearm.v2.using
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.barriers.DistributedBarrier
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.conf.Configuration
import org.gbif.maps.workflow.WorkflowParams
import org.slf4j.LoggerFactory

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
      val overrideParams = WorkflowParams.buildFromOozie(args(2))
      config.hbase.zkQuorum = overrideParams.getZkQuorum
      config.source = overrideParams.getSourceDirectory
      config.pointFeatures.tableName = overrideParams.getTargetTable
      config.tilePyramid.tableName = overrideParams.getTargetTable
      config.targetDirectory = overrideParams.getTargetDirectory
      config.hdfsLockConfig.zkConnectionString = overrideParams.getHdfsLockZkConnectionString
    }

    // setup and read the source
    using(SparkSession.builder().appName(config.appName).getOrCreate()) { spark =>
      import spark.implicits._

      val snapshotName = UUID.randomUUID().toString
      val snapshotPath = createHdfsSnapshot(spark.sparkContext.hadoopConfiguration, config.source, snapshotName, config.hdfsLockConfig)

      try {
        logger.info("Reading Directory {}", config.source)

        val df = spark.read.avro(snapshotPath.toString) // Select only the required columns
          .select($"datasetkey", $"publishingorgkey", $"publishingcountry", $"networkkey", $"countrycode",
                  $"basisofrecord", $"decimallatitude", $"decimallongitude", $"kingdomkey", $"phylumkey", $"classkey",
                  $"orderkey", $"familykey", $"genuskey", $"specieskey", $"taxonkey", $"year", $"v_occurrencestatus",
                  $"hasgeospatialissues") // Filter out records without coordinates, records with issues and absences
          .filter($"decimallatitude".isNotNull && $"decimallongitude".isNotNull && !$"hasgeospatialissues" &&
                  ($"v_occurrencestatus".isNull || !$"v_occurrencestatus".rlike("(?i)absent")))

        logger.info("DataFrame columns are {}", df.columns)

        // get a count of records per mapKey
        val counts = df.flatMap(MapUtils.mapKeysForRecord(_)).rdd.countByValue()

        if (Set("all", "points").contains(args(0))) {
          // upto the threshold we can store points
          val mapKeys = counts.filter(r => {
            r._2 < config.tilesThreshold
          })
          println("MapKeys suitable for storing as point maps: " + mapKeys.size)

          BackfillPoints.build(spark, df, mapKeys.keySet, config)
        }

        if (Set("all", "tiles").contains(args(0))) {
          // above the threshold we build a pyramid
          val mapKeys = counts.filter(r => {
            r._2 >= config.tilesThreshold
          })
          println("MapKeys suitable for creating tile pyramid maps: " + mapKeys.size)

          //val pool = Executors.newFixedThreadPool(config.tilePyramid.projections.length)
          //val jobs : ListBuffer[Callable[Unit]] = new ListBuffer[Callable[Unit]]()
          config.tilePyramid.projections.foreach(proj => {
            println("Building tiles for projection" + proj.srs)

            //jobs += new Callable[Unit] {
            //  override def call() = BackfillTiles.build(sc,df,mapKeys.keySet,config, proj)
            //}
            BackfillTiles.build(spark, df, mapKeys.keySet, config, proj)
          })

          //pool.invokeAll(jobs.toList.asJava)
        }
      } finally {
        deleteHdfsSnapshot(spark.sparkContext.hadoopConfiguration, config.source, snapshotName)
      }
    }
  }


  /**
    * Performs an action in barrier/lock.
    */
  private def doInBarrier[T](config: HdfsLockConfig, action: () => T): T = {
     using(buildCurator(config)) { curator =>
       curator.start()
       val lockPath = config.lockingPath + config.lockName
       val barrier = new DistributedBarrier(curator, lockPath)
       logger.info("Acquiring barrier {}", lockPath)
       barrier.waitOnBarrier()
       logger.info("Setting barrier {}", lockPath)
       barrier.setBarrier()
       val result = action()
       logger.info("Removing barrier {}", lockPath)
       barrier.removeBarrier()
       return result
     }
  }

  /**
    * Creates an non-started instance of {@link CuratorFramework}.
    */
  private def buildCurator(config: HdfsLockConfig): CuratorFramework = CuratorFrameworkFactory.builder.namespace(config.namespace)
    .retryPolicy(new ExponentialBackoffRetry(config.sleepTimeMs, config.maxRetries))
    .connectString(config.zkConnectionString).build


  /**
    * Create a HDFS Snapshot to the input directory.
    */
  private def createHdfsSnapshot(hadoopConfiguration: Configuration, directory: String, snapshotName: String, hdfsLockConfig: HdfsLockConfig ) : Path = {
    using(FileSystem.get(hadoopConfiguration)){ fs =>
      doInBarrier[Path](hdfsLockConfig, () => fs.createSnapshot(new Path(directory), snapshotName))
    }
  }

  /**
    * Deletes a HDFS Snapshot to the input directory.
    */
  private def deleteHdfsSnapshot(hadoopConfiguration: Configuration, directory: String, snapshotName: String ) = {
    using(FileSystem.get(hadoopConfiguration)){ fs =>
      fs.deleteSnapshot(new Path(directory), snapshotName)
    }
  }

  /**
    * Sanitizes application arguments.
    */
  private def checkArgs(args: Array[String]) = {
    assert(args != null && (args.length==2 || args.length==3), usage)
    assert(args(0).equals("all") || args(0).equals("tiles") || args(0).equals("points"), usage)
  }
}
