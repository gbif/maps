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

import scala.collection.mutable;


/**
  * Processes tiles.
  */
object AppWorking3 {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Map tile processing")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Read the source data
    val df = sqlContext.read.parquet("/Users/tim/dev/data/map.parquet")
    df.repartition(12);

    // Dictionary of type to code
    val MAPS_TYPES = Map(
      "TAXON" -> 1,
      "DATASET" -> 2,
      "PUBLISHER" -> 3,
      "COUNTRY" -> 4,
      "PUBLISHING_COUNTRY" -> 5
    )

    // TODO: convention on singleton stuff?
    val zoom = 14.asInstanceOf[Byte]
    val mercator = new Mercator(4096)
    val GEOMETRY_FACTORY = new GeometryFactory()

    val tiles = df.flatMap(row => {
      // note that everything is lowercase (important)
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      val x = mercator.longitudeToTileX(lng, zoom)
      val y = mercator.latitudeToTileY(lat, zoom)
      val px = mercator.longitudeToTileLocalPixelX(lng, zoom)
      val py = mercator.latitudeToTileLocalPixelY(lat, zoom)
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


      // emit a row for every content type that we will group by
      List(
        ((MAPS_TYPES("TAXON"), kingdomKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("TAXON"), phylumKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("TAXON"), classKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("TAXON"), orderKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("TAXON"), familyKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("TAXON"), genusKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("TAXON"), speciesKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("DATASET"), datasetKey, zoom, x, y, px, py, year),1),
        ((MAPS_TYPES("PUBLISHER"), publisherKey, zoom, x, y, px, py, year),1)
      );

    }).reduceByKey((x,y) => (x+y))
    .map( kvp => {
      // regroup to the tile (type,key,zoom,x,y)(px,py,year,count)
      ((kvp._1._1, kvp._1._2, kvp._1._3, kvp._1._4, kvp._1._5),(
        kvp._1._6, kvp._1._7, kvp._1._8, kvp._2
      ))
    })

      // now aggregate by the key to combine data in a tile
      .aggregateByKey(mutable.Map[(Int, Int), mutable.Map[Int,Int]]())(
      (agg, v) => {
        // px:py to year:count
        agg.put((v._1, v._2), mutable.Map(v._3 -> v._4))
        agg
      } ,
      (agg1, agg2) => {
        // merge and convert into a mutable object
        mutable.Map() ++ Maps.merge(agg1, agg2)
      })
      .repartition(32)
      .map(r => {
        // type:key:zoom:x:y as a string
        val key = r._1._1 + ":" +
                  r._1._2 + ":" +
                  r._1._3 + ":" +
                  r._1._4 + ":" +
                  r._1._5


        val encoder = new VectorTileEncoder(4096, 0, false);

        // for each entry (a pixel, px) we have a year:count Map
        r._2.foreach(px => {
          val point = GEOMETRY_FACTORY.createPoint(
            new Coordinate(px._1._1.toDouble, px._1._2.toDouble));
          val meta = new java.util.HashMap[String, Any]()
          // TODO: add years to the VT
          encoder.addFeature("points", meta, point);
        })

        (key,encoder.encode())
      }).sortByKey(true)
      .repartition(32)
      .map(r => {
        val k = new ImmutableBytesWritable(Bytes.toBytes(r._1))
        val row = new KeyValue(
          Bytes.toBytes(r._1),       // key
          Bytes.toBytes("merc"),    // column family
          Bytes.toBytes("tile"),    // cell
          r._2
        )

        (k, row)
      })


    val outputConf = {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "c1n2.gbif.org:2181,c1n3.gbif.org:2181,c1n1.gbif.org:2181");
      conf.setInt("hbase.zookeeper.property.clientPort", 2181);
      val job = new Job(conf, "Map tile builder")
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable]);
      job.setMapOutputValueClass(classOf[KeyValue]);
      val table = new HTable(conf, "maps_v2")
      HFileOutputFormat.configureIncrementalLoad(job, table);
      conf
    }

    tiles
      .saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/tim_v19", classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat], outputConf)
  }
}
