package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.gbif.maps.common.projection.Mercator
import org.gbif.maps.io.PointFeature
import org.apache.spark.mllib.rdd.MLPairRDDFunctions
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Backfill {

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

  private val POINT_THRESHOLD = 100000;
  private val MERCATOR = new Mercator(4096)
  private val GEOMETRY_FACTORY = new GeometryFactory()
  private val MAX_HFILES_PER_CF_PER_REGION = 32 // defined in HBase's LoadIncrementalHFiles
  private val MAX_ZOOM = 4;

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

  def build(sc :SparkContext, df : DataFrame): Unit = {
    //build(sc, df, "TAXON", "kingdomkey")
    //build(sc, df, "TAXON", "phylumkey")
    build(sc, df, "TAXON", "classkey")
    //build(sc, df, "TAXON", "orderkey")
    //build(sc, df, "TAXON", "familykey")
    //build(sc, df, "TAXON", "genuskey")
    //build(sc, df, "TAXON", "specieskey")
  }

  def build(sc : SparkContext, df : DataFrame, mapType : String, sourceField : String): Unit = {
    val initialCounts = df.flatMap(row => {
      if (!row.isNullAt(row.fieldIndex(sourceField))) {
        var year = null.asInstanceOf[Int]
        if (!row.isNullAt(row.fieldIndex("year"))) year = row.getInt(row.fieldIndex("year"))
        List((
          (MAPS_TYPES(mapType),
          row.get(row.fieldIndex(sourceField)),
          row.getDouble(row.fieldIndex("decimallatitude")),
          row.getDouble(row.fieldIndex("decimallongitude")),
          year,
          row.getString(row.fieldIndex("basisofrecord")))
          ,1))
      } else {
        List.empty
      }
    }).reduceByKey(_ + _)

    val largeVals = initialCounts.map(r => {
      ((r._1._1, r._1._2), 1)
    }).reduceByKey(_ + _).filter(r => {r._2 > POINT_THRESHOLD}).collectAsMap().keySet
    // share the lookup among the cluster to allow distributed filtering
    val b_largeVals = sc.broadcast(largeVals)

    //println(mapType + ": initial counts " + initialCounts.count() + " reduced to " + largeVals.size)


    // TODO DECIDE: We can either take a sample (which will result in maps for all) or only prepare point data for
    // those which are not tiled
    //val res = MLPairRDDFunctions.fromPairRDD(pointSource).topByKey(POINT_THRESHOLD).mapValues(r => {


    // prepare the point data filtering out those that need tile pyramids
    val pointSource = initialCounts.filter(r => {!b_largeVals.value.contains((r._1._1, r._1._2))}).map(r => {
      ((r._1._1 + ":" + r._1._2), (r._1._3, r._1._4, r._1._5, r._1._6, r._2))
    })

    val res = pointSource.groupByKey().mapValues(r => {
      val builder = PointFeature.PointFeatures.newBuilder();
      r.foreach(f => {
        val fb = PointFeature.PointFeatures.Feature.newBuilder();
        fb.setLatitude(f._1)
        fb.setLongitude(f._2)
        fb.setYear(f._3)
        fb.setBasisOfRecord(BASIS_OF_RECORD(f._4)) // convert to the protobuf type
        builder.addFeatures(fb)
      })
      builder.build().toByteArray
    }).repartitionAndSortWithinPartitions(new HashPartitioner(32)).map(r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1))
      val row = new KeyValue(Bytes.toBytes(r._1), // key
        Bytes.toBytes("wgs84"), // column family
        Bytes.toBytes("features"), // cell
        r._2 // cell value
      )
      (k, row)
    })
    /*
    res.saveAsNewAPIHadoopFile(TARGET_DIR + "/points/" + sourceField, classOf[ImmutableBytesWritable],
      classOf[KeyValue], classOf[HFileOutputFormat], outputConf)
      */


    // Prepare the RDD for the data to be tiled, limiting to only those which breach the threshold
    val recordsToTile  = initialCounts.filter(r => {b_largeVals.value.contains((r._1._1, r._1._2))}).map(r => {
      (new Tiles.BoRYearRecord(r._1._1, r._1._2, r._1._3, r._1._4, BASIS_OF_RECORD(r._1._6), r._1._5, r._2))
    }).repartition(200).cache()


    /*
    val z4 = Tiles.toMercatorTiles(recordsToTile, 4)
    println("Tiles at z4: " + z4.count())
    val z3 = Tiles.downscale(z4)
    println("Tiles at z3: " + z3.count())
    val z2 = Tiles.downscale(z3)
    println("Tiles at z2: " + z2.count())
    val z1 = Tiles.downscale(z2)
    println("Tiles at z1: " + z1.count())
    val z0 = Tiles.downscale(z1)
    println("Tiles at z0: " + z0.count())
    */

    (0 to 2).foreach(z => {
      var tiles = Tiles.toMercatorTiles(recordsToTile, z)
      persist(Tiles.toVectorTile(tiles), z, sourceField);
    })

    /*
    var tiles = Tiles.toMercatorTiles(recordsToTile, 10)
    persist(Tiles.toVectorTile(tiles), 10, sourceField);
    (0 to 9).reverse.foreach(z => {
      tiles = Tiles.downscale(tiles)
      persist(Tiles.toVectorTile(tiles), z, sourceField);
    })
    */

    // only build the tile cache for those that breach the threshold
    /*
    (0 to MAX_ZOOM).foreach(z => {
      val tilePyramid = initialCounts.filter(r => {b_largeVals.value.contains((r._1._1, r._1._2))}).map(r => {
        val mapType = r._1._1
        val mapKey = r._1._2
        val lat = r._1._3
        val lng = r._1._4
        val year = r._1._5
        val basisOfRecord = r._1._6
        val count = r._2
        val x = MERCATOR.longitudeToTileX(lng, z.asInstanceOf[Byte])
        val y = MERCATOR.latitudeToTileY(lat, z.asInstanceOf[Byte])
        val px = MERCATOR.longitudeToTileLocalPixelX(lng, z.asInstanceOf[Byte])
        val py = MERCATOR.latitudeToTileLocalPixelY(lat, z.asInstanceOf[Byte])
        ((mapType, mapKey, z, x, y),(px, py,year, count))
      }).aggregateByKey(
        // now aggregate by the key to combine data in a tile
        mutable.Map[(Int, Int), mutable.Map[Int, Int]]())((agg, v) => {
        // px:py to year:count
        agg.put((v._1, v._2), mutable.Map(v._3 -> v._4))
        agg
      }, (agg1, agg2) => {
        // merge and convert into a mutable object
        mutable.Map() ++ Maps.merge(agg1, agg2)
      })

      persist(tilePyramid, z, sourceField);
    })
    */
  }

  def persist(tilePyramid: RDD[(Tiles.TileKey, Array[Byte])], z: Int, sourceField: String) = {
    tilePyramid.repartitionAndSortWithinPartitions(new HashPartitioner(MAX_HFILES_PER_CF_PER_REGION)).map(t => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(t._1.mapType + ":" + t._1.mapKey))
      val cell = t._1.z + ":" + t._1.x + ":" + t._1.y
      val cellData = t._2
      val row = new KeyValue(Bytes.toBytes(t._1.mapType + ":" + t._1.mapKey), // key
        Bytes.toBytes("merc_tiles"), // column family
        Bytes.toBytes(cell), // cell
        cellData)
      (k, row)
    }).saveAsNewAPIHadoopFile(TARGET_DIR + "/tiles/" + sourceField + "/" + z, classOf[ImmutableBytesWritable],
      classOf[KeyValue], classOf[HFileOutputFormat], outputConf)
  }
}
