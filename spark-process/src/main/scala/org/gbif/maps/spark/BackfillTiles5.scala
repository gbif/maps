package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.gbif.maps.common.projection.Mercator
import org.gbif.maps.io.PointFeature
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.immutable.Nil
import scala.collection.mutable

object BackfillTiles5 {


  // Dictionary of map types
  private val MAPS_TYPES = Map("ALL" -> 0, "TAXON" -> 1, "DATASET" -> 2, "PUBLISHER" -> 3, "COUNTRY" -> 4,
    "PUBLISHING_COUNTRY" -> 5)

  // Dictionary mapping the GBIF API BasisOfRecord enumeration to the Protobuf versions
  private val BASIS_OF_RECORD = Map("UNKNOWN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.UNKNOWN,
    "PRESERVED_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.PRESERVED_SPECIMEN,
    "FOSSIL_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.FOSSIL_SPECIMEN,
    "LIVING_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.LIVING_SPECIMEN,
    "OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.OBSERVATION,
    "HUMAN_OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.HUMAN_OBSERVATION,
    "MACHINE_OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.MACHINE_OBSERVATION,
    "MATERIAL_SAMPLE" -> PointFeature.PointFeatures.Feature.BasisOfRecord.MATERIAL_SAMPLE,
    "LITERATURE" -> PointFeature.PointFeatures.Feature.BasisOfRecord.LITERATURE)

  val POINT_THRESHOLD = 100000;
  val MERCATOR = new Mercator(4096)
  val GEOMETRY_FACTORY = new GeometryFactory()
  val MAX_HFILES_PER_CF_PER_REGION = 32 // defined in HBase's LoadIncrementalHFiles
  val MAX_ZOOM = 4 // 4 worked well - killed 8 at around 40 mins (50%)
  val MIN_ZOOM = 0

  private val TARGET_DIR = "hdfs://c1n1.gbif.org:8020/tmp/tim_maps"

  /**
    * TODO: fix this
    * Write to the dev cluster for now - always
    */
  private val outputConf = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "c1n2.gbif.org:2181,c1n3.gbif.org:2181,c1n1.gbif.org:2181");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    val job = new Job(conf, "Map tile builder")
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable]);
    job.setMapOutputValueClass(classOf[KeyValue]);
    val table = new HTable(conf, "tim_test")
    HFileOutputFormat.configureIncrementalLoad(job, table);
    conf
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map processing")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setIfMissing("spark.master", "local[2]") // 2 threads for local dev, ignored in production
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/user/hive/warehouse/tim.db/occurrence_map_source")
    //val df = sqlContext.read.parquet("/Users/tim/dev/data/map.parquet")
    build(sc, df)
  }

  def largeViews(df : DataFrame) : Map[(Int,Any),Long] = {
    var largeViews = df.flatMap(row => {
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      val bor = BASIS_OF_RECORD(row.getString(row.fieldIndex("basisofrecord")))
      val year = if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Int]
      else row.getInt((row.fieldIndex("year")))
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
        ((MAPS_TYPES("ALL"), 0), 1),
        ((MAPS_TYPES("DATASET"), datasetKey), 1),
        ((MAPS_TYPES("PUBLISHER"), publisherKey), 1),
        ((MAPS_TYPES("COUNTRY"), country), 1),
        ((MAPS_TYPES("PUBLISHING_COUNTRY"), publishingCountry), 1)
      )

      taxonIDs.foreach(id => {
        res += (((MAPS_TYPES("TAXON"), id), 1))
      })
      res
    }).countByKey().filter(r => {r._2>=POINT_THRESHOLD}).toMap

    largeViews.foreach(r => {println("Tiling Type[" + r._1._1 + "] Key[" + r._1._2 + "] with [" + r._2 + "] records")})
    largeViews
  }

  def build(sc :SparkContext, df : DataFrame): Unit = {

    // Determine and broadcast which of the views we consider suitable for tiling
    val keysToTile = sc.broadcast(largeViews(df).keySet)

    val tiles = df.flatMap(row => {
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      val bor = BASIS_OF_RECORD(row.getString(row.fieldIndex("basisofrecord")))
      val year = if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Int]
      else row.getInt((row.fieldIndex("year")))
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

      val res = mutable.ArrayBuffer[((Int,Any,Int,Long,Long,Int,Int,Int,Feature.BasisOfRecord),Int)]()
      (MIN_ZOOM to MAX_ZOOM).map(zoom => {
        //val zoom = MAX_ZOOM
        val z = zoom.asInstanceOf[Byte]
        val x = MERCATOR.longitudeToTileX(lng, z)
        val y = MERCATOR.latitudeToTileY(lat, z)
        val px = MERCATOR.longitudeToTileLocalPixelX(lng, z)
        val py = MERCATOR.latitudeToTileLocalPixelY(lat, z)

        if (keysToTile.value.contains((MAPS_TYPES("ALL"), 0)))
          res += (((MAPS_TYPES("ALL"), 0, z, x, y, px, py, year, bor), 1))
        if (keysToTile.value.contains((MAPS_TYPES("DATASET"), datasetKey)))
          res += (((MAPS_TYPES("DATASET"), datasetKey, z, x, y, px, py, year, bor), 1))
        if (keysToTile.value.contains((MAPS_TYPES("PUBLISHER"), publisherKey)))
          res += (((MAPS_TYPES("PUBLISHER"), publisherKey, z, x, y, px, py, year, bor), 1))
        if (keysToTile.value.contains((MAPS_TYPES("COUNTRY"), country)))
          res += (((MAPS_TYPES("COUNTRY"), country, z, x, y, px, py, year, bor), 1))
        if (keysToTile.value.contains((MAPS_TYPES("PUBLISHING_COUNTRY"), publishingCountry)))
          res += (((MAPS_TYPES("PUBLISHING_COUNTRY"), publishingCountry, z, x, y, px, py, year, bor), 1))

        taxonIDs.foreach(id => {
          if (keysToTile.value.contains((MAPS_TYPES("TAXON"), id)))
            res += (((MAPS_TYPES("TAXON"), id, z, x, y, px, py, year, bor), 1))
        })
      })
      res

      //}).reduceByKey(_ + _).map(r => {  // FAILED!
      //}).repartition(1000).reduceByKey(_ + _).map(r => { // took 20 mins to reparition, 40 in total
    //}).reduceByKey(_ + _, 500).map(r => { // 23 minutes in total into HBase(!) - points only
    //}).reduceByKey(_ + _, 250).map(r => { // 23 minutes in total into HBase(!) - points only
    }).map(r => { // 18 minutes into HBase(!) - points only  (17mins, 46sec for all Z 0 to 4 into HBase)
      // regroup to the tile(typeKey, ZXY) : pixel+features
      ((r._1._1 + ":" + r._1._2, r._1._3 + ":" + r._1._4 + ":" + r._1._5), (r._1._6, r._1._7, r._1._8, r._1._9, r._2))
    })

    // Notes to Tim:
    // [tim@prodgateway-vh ~]$ ~/spark/bin/spark-submit --master yarn --jars $HIVE_CLASSPATH /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar --num-executors 25 --executor-memory 10g --executor-cores 5  --deploy-mode cluster --class "org.gbif.maps.spark.BackfillTiles5" spark-process-0.1-SNAPSHOT.jar
    // println("Total record count: " + tiles.count())
    // On a quiet cluster (total job time, including approx 1 min initial count):
    // 0 -> 1 = Total record count: 1,259,651,030
    // 15 -> 15 = Total record count: 987,290,949  (a. 11mins, 35sec   b.  11mins, 17sec)
    // Timings above not including the Encoding or saving as HFiles

    // Suspect timings above were clever enough to not do the reduce by key!!!
    // Yes - started failing when adding the encoding bits.
    // Repartitioning to 1000: took 22 mins, and shuffled 130GB twice - seen on ganglia
    // Perhaps that included the reduceByKey()?


    // collect the pixels per tile (for a test here only - need to collect properly(!))
    // option 1: collect to the final structure
    // option 2: push a collection of data to tile in the encoding bit later (suggest this first)

    val appendVal = (m: mutable.Set[(Int,Int)], v: (Int,Int,Int,Feature.BasisOfRecord,Int)) => {m+=((v._1, v._2))}
    val merge = (m1: mutable.Set[(Int,Int)], m2: mutable.Set[(Int,Int)]) => {m1++=m2}
    val tiles2 = tiles.aggregateByKey(mutable.Set[(Int,Int)]().empty)(appendVal, merge)

    tiles2.mapValues(tile => {
      val pixels = mutable.Set[(Int,Int)]().empty
      tile.foreach(p => {pixels+=((p._1,p._2))})
      val encoder = new VectorTileEncoder(4096, 0, false); // for each entry (a pixel, px) we have a year:count Map

      pixels.foreach(pixel => {
        val point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel._1.toDouble, pixel._2.toDouble));
        val meta = new java.util.HashMap[String, Any]() // TODO: add metadata(!)
        encoder.addFeature("points", meta, point);
      })
      encoder.encode()
    }).repartitionAndSortWithinPartitions(new HashPartitioner(MAX_HFILES_PER_CF_PER_REGION)).map( r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1._1))
      val cell = r._1._2
      val cellData = r._2
      val row = new KeyValue(Bytes.toBytes(r._1._1), // key
        Bytes.toBytes("merc_tiles"), // column family
        Bytes.toBytes(cell), // cell
        cellData)
      (k, row)
    }).saveAsNewAPIHadoopFile(TARGET_DIR + "/tiles/z" + MAX_ZOOM, classOf[ImmutableBytesWritable],
      classOf[KeyValue], classOf[HFileOutputFormat], outputConf)

  }
}

