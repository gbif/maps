package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.gbif.maps.common.projection.Mercator
import org.gbif.maps.io.PointFeature
import scala.collection.mutable;


/**
  * Processes tiles.
  */
object AppPoints3 {

  object VectorTileEncoderFactory {
    def newEncoder(): VectorTileEncoder = {
      new VectorTileEncoder(4066, 0, true)
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map tile processing").setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Read the source data
    //val df = sqlContext.read.parquet("/Users/tim/dev/data/map.parquet")
    //val df = sqlContext.read.parquet("/Users/tim/dev/data/all_point1_deg")
    val df = sqlContext.read.parquet("/user/hive/warehouse/tim.db/occurrence_map_source_sample")

    df.repartition(100);

    // Number of points to put into the single cell in HBase.
    // Maps with more features than this will be pushed into a tile pyramid
    val pointThreshold = 100000;
    val mercator = new Mercator(4096)
    val GEOMETRY_FACTORY = new GeometryFactory()

    // Dictionary of type to code
    // TODO: Perhaps a Java API?
    val MAPS_TYPES = Map("ALL" -> 0, "TAXON" -> 1, "DATASET" -> 2, "PUBLISHER" -> 3, "COUNTRY" -> 4,
      "PUBLISHING_COUNTRY" -> 5)
    val BASIS_OF_RECORD = Map("UNKNOWN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.UNKNOWN,
      "PRESERVED_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.PRESERVED_SPECIMEN,
      "FOSSIL_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.FOSSIL_SPECIMEN,
      "LIVING_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.LIVING_SPECIMEN,
      "OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.OBSERVATION,
      "HUMAN_OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.HUMAN_OBSERVATION,
      "MACHINE_OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.MACHINE_OBSERVATION,
      "MATERIAL_SAMPLE" -> PointFeature.PointFeatures.Feature.BasisOfRecord.MATERIAL_SAMPLE,
      "LITERATURE" -> PointFeature.PointFeatures.Feature.BasisOfRecord.LITERATURE)

    val initialCounts = df.flatMap(row => {
      // note that everything is lowercase (important)
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      val year = row.getInt(row.fieldIndex("year"))
      val datasetKey = row.getString(row.fieldIndex("datasetkey"))
      val publisherKey = row.getString(row.fieldIndex("publishingorgkey"))
      val kingdomKey = row.getInt(row.fieldIndex("kingdomkey"))
      val phylumKey = row.getInt(row.fieldIndex("phylumkey"))
      val classKey = row.getInt(row.fieldIndex("classkey"))
      val orderKey = row.getInt(row.fieldIndex("orderkey"))
      val familyKey = row.getInt(row.fieldIndex("familykey"))
      val genusKey = row.getInt(row.fieldIndex("genuskey"))
      val speciesKey = row.getInt(row.fieldIndex("specieskey"))
      // TODO: taxonKey stuff (!)
      val basisOfRecord = row.getString(row.fieldIndex("basisofrecord"))
      //val country = row.getString(row.fieldIndex("country"))
      //val publishingCountry = row.getInt(row.fieldIndex("publishingCountry"))
      // TODO: all the types
      List(((MAPS_TYPES("DATASET"), datasetKey, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("TAXON"), speciesKey, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("TAXON"), kingdomKey, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("ALL"), 0, lat, lng, year, basisOfRecord), 1))
    }).reduceByKey((x, y) => (x + y))

    initialCounts.cache();

    // take the First N records from each group and collect them suitable for HFile writing
    val pointData = initialCounts.map(r => {
      ((r._1._1, r._1._2), (r._1._3, r._1._4, r._1._5, r._1._6))

    }).groupByKey().map(r => {
      (r._1, r._2.take(pointThreshold))

    }).map(r => {
      val key = r._1._1 + ":" + r._1._2
      val builder = PointFeature.PointFeatures.newBuilder();
      r._2.foreach(f => {
        val fb = PointFeature.PointFeatures.Feature.newBuilder();
        fb.setLatitude(f._1)
        fb.setLongitude(f._2)
        fb.setYear(f._3)
        fb.setBasisOfRecord(BASIS_OF_RECORD(f._4)) // convert to the protobuf type
        builder.addFeatures(fb)
      })

      val value = r._2.iterator.mkString(",")
      (key, builder.build().toByteArray)
    }).repartition(32).sortByKey(true).map(r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1))
      val row = new KeyValue(Bytes.toBytes(r._1), // key
        Bytes.toBytes("wgs84"), // column family
        Bytes.toBytes("features"), // cell
        r._2 // cell value
      )
      (k, row)
    })

    // build an index of all the entities that need built into a tile pyramid
    val largeVals = initialCounts.map(r => {
      ((r._1._1, r._1._2), 1)
    }).reduceByKey(_ + _).filter(r => {
      r._2 > pointThreshold
    })
    largeVals.foreach(println)

    // share the lookup among the cluster to allow distributed filtering
    val b_largeVals = sc.broadcast(largeVals.collectAsMap().toMap)

    // only build the tile cache for those that breach the threshold
    val tilePyramid = initialCounts.filter(r => {b_largeVals.value.contains((r._1._1, r._1._2))}).flatMap(r => {
        val mapType = r._1._1
        val mapKey = r._1._2
        val lat = r._1._3
        val lng = r._1._4
        val year = r._1._5
        val basisOfRecord = r._1._6
        val count = r._2

        (0 to 1).flatMap(z => {
          // TODO: BoR
          val x = mercator.longitudeToTileX(lng, z.asInstanceOf[Byte])
          val y = mercator.latitudeToTileY(lat, z.asInstanceOf[Byte])
          val px = mercator.longitudeToTileLocalPixelX(lng, z.asInstanceOf[Byte])
          val py = mercator.latitudeToTileLocalPixelY(lat, z.asInstanceOf[Byte])

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
    }).repartition(32).sortByKey(true).map(r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1._1))
      val cell = r._1._2
      val cellData = r._2
      val row = new KeyValue(Bytes.toBytes(r._1._1), // key
        Bytes.toBytes("merc_tiles"), // column family
        Bytes.toBytes(cell), // cell
        cellData)

      (k, row)
    })



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

    pointData.saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/zp6", classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat], outputConf)

    tilePyramid.saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/zt6", classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat], outputConf)

  }
}
