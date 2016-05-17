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
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.gbif.maps.common.model.CategoryDensityTile
import org.gbif.maps.common.projection.Mercator
import org.gbif.maps.io.PointFeature
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}

object BackfillTiles10 {

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
  private val TILE_SIZE = 4096
  private val MERCATOR = new Mercator(TILE_SIZE)
  private val GEOMETRY_FACTORY = new GeometryFactory()
  private val MAX_HFILES_PER_CF_PER_REGION = 32 // defined in HBase's LoadIncrementalHFiles
  private val MAX_ZOOM = 15
  private val MIN_ZOOM = 0


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
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setIfMissing("spark.master", "local[2]") // 2 threads for local dev, ignored in production
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet("/user/hive/warehouse/tim.db/occurrence_map_source")
    build(sc, df)
  }

  def encodePixel(x: Short, y: Short): Int = {
    x.toInt << 16 | y
  }

  def decodePixel(p: Int): (Short,Short) = {
    ((p >> 16).asInstanceOf[Short], p.asInstanceOf[Short])
  }

  def encodePixelYear(p: Int, y: Short): Long = {
    p.toLong << 32 | y
  }

  def decodePixelYear(py: Long): (Int,Short) = {
    ((py >> 32).asInstanceOf[Int], py.asInstanceOf[Short])
  }


  def build(sc :SparkContext, df : DataFrame): Unit = {

    var tiles = df.flatMap(row => {
      val res = mutable.ArrayBuffer[((String, String, Int, Feature.BasisOfRecord, Short),Int)]()
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      if (lat >= -85 && lat <= 85) {
        val lng = row.getDouble(row.fieldIndex("decimallongitude"))
        val bor = BASIS_OF_RECORD(row.getString(row.fieldIndex("basisofrecord")))
        val year = if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Short]
        else row.getInt((row.fieldIndex("year"))).asInstanceOf[Short]

        val z = MAX_ZOOM.asInstanceOf[Byte]
        val x = MERCATOR.longitudeToTileX(lng, z).asInstanceOf[Int] // max z15!
        val y = MERCATOR.latitudeToTileY(lat, z).asInstanceOf[Int]

        val px = MERCATOR.longitudeToTileLocalPixelX(lng, z).asInstanceOf[Short]
        val py = MERCATOR.latitudeToTileLocalPixelY(lat, z).asInstanceOf[Short]
        val pixel = encodePixel(px, py)

        val key = z + ":" + x + ":" + y
        res += ((("0:0", key, pixel, bor, year), 1))

        // TODO: this stuff(!)
        if (!row.isNullAt(row.fieldIndex("kingdomkey"))){
          val taxon = row.getInt((row.fieldIndex("kingdomkey")))
          res += ((("1:" + taxon, key, pixel, bor, year), 1))
        }
        if (!row.isNullAt(row.fieldIndex("classkey"))){
          val taxon = row.getInt((row.fieldIndex("classkey")))
          res += ((("1:" + taxon, key, pixel, bor, year), 1))
        }
        if (!row.isNullAt(row.fieldIndex("orderkey"))){
          val taxon = row.getInt((row.fieldIndex("orderkey")))
          res += ((("1:" + taxon, key, pixel, bor, year), 1))
        }
        if (!row.isNullAt(row.fieldIndex("datasetkey"))){
          val id = row.getString((row.fieldIndex("datasetkey")))
          res += ((("2:" + id, key, pixel, bor, year), 1))
        }


      }
      res
    }).reduceByKey(_+_, 200).map(r => {
      // type, zxy, bor : pixel,year,count
      ((r._1._1, r._1._2, r._1._4),(r._1._3,r._1._5,r._2))
    })

    // collect a map of pixelYear -> count
    val appendVal = (m: MMap[Long,Int], v: (Int,Short,Int)) => {
      val py = encodePixelYear(v._1,v._2)
      m+=((py, v._3))
    }
    val merge = (m1: MMap[Long,Int], m2: MMap[Long,Int]) => {
      // deep copy
      var result = MMap[Long,Int]()
      result ++= m1
      m2.foreach(e => {
        result(e._1) = result.getOrElse(e._1, 0) + e._2
      })
      result
    }
    val tiles2 = tiles.aggregateByKey(MMap[Long,Int]().empty)(appendVal, merge)

    // collect a map of category -> pixelYear -> count
    val appendVal2 = (m: MMap[Feature.BasisOfRecord,MMap[Long,Int]], v: (Feature.BasisOfRecord,MMap[Long,Int])) => {m+=((v._1, v._2))}
    val merge2 = (m1: MMap[Feature.BasisOfRecord,MMap[Long,Int]], m2:MMap[Feature.BasisOfRecord,MMap[Long,Int]]) => {
      // deep copy
      var result = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
      result ++= m1
      m2.foreach(e => {
        val bor = result.getOrElseUpdate(e._1, MMap[Long,Int]())
        e._2.foreach( f => {
          bor(f._1) = bor.getOrElse(f._1, 0) + f._2
        })
      })
      result

    }
    var tiles3 = tiles2.map(r => {
      ((r._1._1,r._1._2),(r._1._3, r._2))
    }).aggregateByKey(MMap[Feature.BasisOfRecord,MMap[Long,Int]]().empty)(appendVal2, merge2)

    (MIN_ZOOM to MAX_ZOOM).reverse.foreach(z => {

      // downscale if needed
      if (z != MAX_ZOOM) {

        tiles3 = tiles3.map(t => {
          val zxy = t._1._2.split(":")
          val zoom = zxy(0).toInt
          val x = zxy(1).toInt
          val y = zxy(2).toInt

          val newTile = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
          t._2.foreach(keyVal => {
            val bor = newTile.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
            keyVal._2.foreach(feature => {
              val decoded = decodePixelYear(feature._1)
              val pixel = decodePixel(decoded._1)
              val year = decoded._2
              val px = (pixel._1/2 + TILE_SIZE/2 * (x%2)).asInstanceOf[Short]
              val py = (pixel._2/2 + TILE_SIZE/2 * (y%2)).asInstanceOf[Short]
              val newKey = encodePixelYear(encodePixel(px,py), year)

              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            })
          })

          ((t._1._1, z + ":" + x/2 + ":" + y/2), newTile)

        }).reduceByKey((a,b) => {
          // deep copy
          var result = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
          result ++= a
          b.foreach(e => {
            val bor = result.getOrElseUpdate(e._1, MMap[Long,Int]())
            e._2.foreach( f => {
              bor(f._1) = bor.getOrElse(f._1, 0) + f._2
            })
          })
          result
        })
      }

      // write the tile
      tiles3.mapValues(tile => {
        val encoder = new VectorTileEncoder(TILE_SIZE, 0, false);

        tile.keySet.foreach(bor => {
          val pixelYears = tile.get(bor)

          pixelYears.get.foreach(py => {
            val x = decodePixelYear(py._1)
            val pixel = decodePixel(x._1)
            val year = x._2

            val point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel._1.toDouble, pixel._2.toDouble));
            val meta = new java.util.HashMap[String, Any]() // TODO: add metadata(!)

            encoder.addFeature("points", meta, point);
          })
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
      }).saveAsNewAPIHadoopFile(TARGET_DIR + "/tiles/z" + z, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], outputConf)
    })

  }
}

