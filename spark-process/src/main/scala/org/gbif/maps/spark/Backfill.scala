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
    val conf = new SparkConf().setAppName("Map processing").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/user/hive/warehouse/tim.db/occurrence_map_source")
    build(sc, df)
  }

  def build(sc :SparkContext, df : DataFrame): Unit = {
    build(sc, df, "TAXON", "genuskey")
  }

  def persist(tilePyramid: RDD[((Int, Any, Int, Long, Long), mutable.Map[(Int, Int), mutable.Map[Int, Int]])],
    z: Int, sourceField: String) = {
    val tiles = tilePyramid.map(r => {
      // type:key:zoom:x:y as a string
      val key = r._1._1 + ":" + r._1._2
      val encoder = new VectorTileEncoder(4096, 0, false);
      //val encoder = () => VectorTileEncoderFactory.newEncoder()
      // for each entry (a pixel, px) we have a year:count Map
      r._2.foreach(px => {
        val point = GEOMETRY_FACTORY.createPoint(new Coordinate(px._1._1.toDouble, px._1._2.toDouble));
        val meta = new java.util.HashMap[String, Any]()
        // TODO: add years to the VT
        encoder.addFeature("points", meta, point);
      })
      //val result = encoder.encode()
      ((key, r._1._3 + ":" + r._1._4 + ":" + r._1._5), encoder.encode())
    }).repartitionAndSortWithinPartitions(new HashPartitioner(MAX_HFILES_PER_CF_PER_REGION)).map(r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1._1))
      val cell = r._1._2
      val cellData = r._2
      val row = new KeyValue(Bytes.toBytes(r._1._1), // key
        Bytes.toBytes("merc_tiles"), // column family
        Bytes.toBytes(cell), // cell
        cellData)
      (k, row)
    })
    tiles.saveAsNewAPIHadoopFile(TARGET_DIR + "/tiles/" + sourceField + "/" + z, classOf[ImmutableBytesWritable],
      classOf[KeyValue], classOf[HFileOutputFormat], outputConf)
  }

  def build(sc : SparkContext, df : DataFrame, mapType : String, sourceField : String): Unit = {
    val initialCounts = df.flatMap(row => {
      if (!row.isNullAt(row.fieldIndex(sourceField))) {
        var year = null.asInstanceOf[Int]
        if (!row.isNullAt(row.fieldIndex("year"))) year = row.getInt(row.fieldIndex("year"))
        List(((MAPS_TYPES(mapType),
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


    // prepare the point data filtering out those that need tile pyramids
    val pointSource = initialCounts.filter(r => {!b_largeVals.value.contains((r._1._1, r._1._2))}).map(r => {
      ((r._1._1 + ":" + r._1._2), (r._1._3, r._1._4, r._1._5, r._1._6, r._2))
    })

    val res = MLPairRDDFunctions.fromPairRDD(pointSource).topByKey(POINT_THRESHOLD).mapValues(r => {
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
    res.saveAsNewAPIHadoopFile(TARGET_DIR + "/points/" + sourceField, classOf[ImmutableBytesWritable],
      classOf[KeyValue], classOf[HFileOutputFormat], outputConf)

    /*
    val tiles = initialCounts.filter(r => {b_largeVals.value.contains((r._1._1, r._1._2))}).map(r => {
      val mapType = r._1._1
      val mapKey = r._1._2
      val lat = r._1._3
      val lng = r._1._4
      val year = r._1._5
      val basisOfRecord = r._1._6
      val count = r._2
      val x = MERCATOR.longitudeToTileX(lng, MAX_ZOOM.asInstanceOf[Byte])
      val y = MERCATOR.latitudeToTileY(lat, MAX_ZOOM.asInstanceOf[Byte])
      val px = MERCATOR.longitudeToTileLocalPixelX(lng, MAX_ZOOM.asInstanceOf[Byte])
      val py = MERCATOR.latitudeToTileLocalPixelY(lat, MAX_ZOOM.asInstanceOf[Byte])
      ((mapType, mapKey, MAX_ZOOM, x, y),(px, py,year, count))
    })

    (MAX_ZOOM to 0).foreach(z => {
      // downscale if requires
      if (z < MAX_ZOOM) {
        tiles = tiles.map(t => {
          val oldX = t._1._4;
          val oldY = t._1._5;
          val x = (t._1._4/2).asInstanceOf[Int];
          val y = (t._1._5/2).asInstanceOf[Int];
          val px = (t._2._1/2).asInstanceOf[Int] + (oldX-x)

          ((t._1._1, t._1._2, t._1._3,(t._1._4/2).asInstanceOf[Int], (t._1._5/2).asInstanceOf[Int]),
            (t._2._1 + ))
        })
      }
      val t = tiles.aggregateByKey(
        // now aggregate by the key to combine data in a tile
        mutable.Map[(Int, Int), mutable.Map[Int, Int]]())((agg, v) => {
        // px:py to year:count
        agg.put((v._1, v._2), mutable.Map(v._3 -> v._4))
        agg
      }, (agg1, agg2) => {
        // merge and convert into a mutable object
        mutable.Map() ++ Maps.merge(agg1, agg2)
      })

      persist(t, z, sourceField);

    })
    */

    // only build the tile cache for those that breach the threshold
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

  }
}
