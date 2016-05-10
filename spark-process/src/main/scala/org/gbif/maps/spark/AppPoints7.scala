package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.gbif.maps.common.projection.Mercator
import org.gbif.maps.io.PointFeature

import scala.collection.mutable


/**
  * Processes tiles.
  */
object AppPoints7 {

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

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map processing").setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Number of points to put into the single cell in HBase.
    // Maps with more features than this will be pushed into a tile pyramid
    val POINT_THRESHOLD = 100000;
    val MERCATOR = new Mercator(4096)
    val GEOMETRY_FACTORY = new GeometryFactory()
    val MAX_HFILES_PER_CF_PER_REGION = 32 // defined in HBase's LoadIncrementalHFiles
    val MAX_ZOOM = 4;

    // Read the source data
    //val df = sqlContext.read.parquet("/user/hive/warehouse/tim.db/occurrence_map_source")
    val df = sqlContext.read.parquet("/Users/tim/dev/data/map.parquet")

    val initialCounts = df.flatMap(row => {
      // note that everything is lowercase when created in Hive (important)
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      var year = null.asInstanceOf[Int]
      if (!row.isNullAt(row.fieldIndex("year"))) year = row.getInt(row.fieldIndex("year"))
      val basisOfRecord = row.getString(row.fieldIndex("basisofrecord"))

      val datasetKey = row.getString(row.fieldIndex("datasetkey"))
      val publisherKey = row.getString(row.fieldIndex("publishingorgkey"))
      val country = row.getString(row.fieldIndex("countrycode"))
      val publishingCountry = row.getString(row.fieldIndex("publishingcountry"))

      // distinct the taxa keys by which the occurrence has been identified
      var taxonIDs = Set[Int]()
      if (!row.isNullAt(row.fieldIndex("kingdomkey"))) taxonIDs+=row.getInt(row.fieldIndex("kingdomkey"))
      if (!row.isNullAt(row.fieldIndex("phylumkey"))) taxonIDs+=row.getInt(row.fieldIndex("phylumkey"))
      if (!row.isNullAt(row.fieldIndex("classkey"))) taxonIDs+=row.getInt(row.fieldIndex("classkey"))
      if (!row.isNullAt(row.fieldIndex("orderkey"))) taxonIDs+=row.getInt(row.fieldIndex("orderkey"))
      if (!row.isNullAt(row.fieldIndex("familykey"))) taxonIDs+=row.getInt(row.fieldIndex("familykey"))
      if (!row.isNullAt(row.fieldIndex("genuskey"))) taxonIDs+=row.getInt(row.fieldIndex("genuskey"))
      if (!row.isNullAt(row.fieldIndex("specieskey"))) taxonIDs+=row.getInt(row.fieldIndex("specieskey"))
      if (!row.isNullAt(row.fieldIndex("taxonkey"))) taxonIDs+=row.getInt(row.fieldIndex("taxonkey"))

      val m1 = mutable.ArrayBuffer[((Int,Any,Double,Double,Int,String),Int)](
        ((MAPS_TYPES("ALL"), 0, lat, lng, year, basisOfRecord),1)
        /*((MAPS_TYPES("DATASET"), datasetKey, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("PUBLISHER"), publisherKey, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("COUNTRY"), country, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("PUBLISHING_COUNTRY"), publishingCountry, lat, lng, year, basisOfRecord), 1)*/
      )

      // add the distinct taxa
      /*
      taxonIDs.foreach(id => {
        m1 += (((MAPS_TYPES("TAXON"), id, lat, lng, year, basisOfRecord), 1))
      })
      */

      m1
    }).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val pointData = initialCounts.map(r => {
      ((r._1._1 + ":" + r._1._2), (r._1._3, r._1._4, r._1._5, r._1._6, r._2))

    }).aggregateByKey(new BoundedList[(Double, Double, Int, String, Int)](POINT_THRESHOLD))((agg, v) => {
      agg.appendElem(v)
      agg
    },((agg1,agg2) => {
      agg1++=agg2 // (append all of agg2 to agg1)
      agg1
    })).map(r => {
        val builder = PointFeature.PointFeatures.newBuilder();
        r._2.foreach(f => {
          val fb = PointFeature.PointFeatures.Feature.newBuilder();
          fb.setLatitude(f._1)
          fb.setLongitude(f._2)
          fb.setYear(f._3)
          fb.setBasisOfRecord(BASIS_OF_RECORD(f._4)) // convert to the protobuf type
          builder.addFeatures(fb)
        })
        (r._1, builder.build().toByteArray)
      }).repartitionAndSortWithinPartitions(new HashPartitioner(32)).map(r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1))
      val row = new KeyValue(Bytes.toBytes(r._1), // key
        Bytes.toBytes("wgs84"), // column family
        Bytes.toBytes("features"), // cell
        r._2 // cell value
      )
      (k, row)
    })

    println("points: " + pointData.count())


    // find the maps that have enough features to trigger the building of a tile pyramid
    /*
    val largeVals = initialCounts.map(r => {
      ((r._1._1, r._1._2), 1)
    }).reduceByKey(_ + _).filter(r => {r._2 > POINT_THRESHOLD}).collectAsMap().keySet

    // share the lookup among the cluster to allow distributed filtering
    val b_largeVals = sc.broadcast(largeVals)

    // only build the tile cache for those that breach the threshold
    val tilePyramid = initialCounts.filter(r => {b_largeVals.value.contains((r._1._1, r._1._2))}).flatMap(r => {
        val mapType = r._1._1
        val mapKey = r._1._2
        val lat = r._1._3
        val lng = r._1._4
        val year = r._1._5
        val basisOfRecord = r._1._6
        val count = r._2

        (0 to MAX_ZOOM).flatMap(z => {
          // TODO: BoR
          val x = MERCATOR.longitudeToTileX(lng, z.asInstanceOf[Byte])
          val y = MERCATOR.latitudeToTileY(lat, z.asInstanceOf[Byte])
          val px = MERCATOR.longitudeToTileLocalPixelX(lng, z.asInstanceOf[Byte])
          val py = MERCATOR.latitudeToTileLocalPixelY(lat, z.asInstanceOf[Byte])

          List(((mapType, mapKey, z, x, y, px, py, year), count));
        })
      }).map(r => {
        // regroup to the tile (type,key,zoom,x,y)(px,py,year,count)
        ((r._1._1, r._1._2, r._1._3, r._1._4, r._1._5), (r._1._6, r._1._7, r._1._8, r._2))
      }).aggregateByKey(
        // now aggregate by the key to combine data in a tile
        mutable.Map[(Int, Int), mutable.Map[Int, Int]]())((agg, v) => {
        // px:py to year:count
          agg.put((v._1, v._2), mutable.Map(v._3 -> v._4))
          agg
        }, (agg1, agg2) => {
      // merge and convert into a mutable object
      mutable.Map() ++ Maps.merge(agg1, agg2)
    }).map(r => {
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
    */


    //println("tiles: " + tilePyramid.count())
    /*

    val outputConf = {
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

    pointData.saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/del_zp1", classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat], outputConf)

    tilePyramid.saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/del_zt14", classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat], outputConf)
        */

  }
}
