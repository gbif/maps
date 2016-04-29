package org.gbif.maps.spark

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.gbif.maps.common.projection.Mercator

import scala.collection.mutable;


/**
  * Processes tiles.
  */
object AppWorking2 {


  case class Pixel(val x: Long, val y: Long, val px: Int, val py: Int, val year: Int);

  // The types of map are defined as:
  //  1. taxon
  //  2. dataset
  //  3. publisher
  //  4. country
  //  5. publishingCountry
  case class TileKey(val contentType: Byte, val zoom: Byte, val x: Long, val y: Long);
  case class PixelFeature(val px: Int, val py: Int, val year: Int, val count: Int);

  def toTile(row: Row, z: Byte): Pixel = {
    val mercator = new Mercator(4096);
    val lat = row.getDouble(row.fieldIndex("decimallatitude"));
    val lng = row.getDouble(row.fieldIndex("decimallongitude"));
    val year = row.getInt(row.fieldIndex("year"));

    return new Pixel(mercator.longitudeToTileX(lng, z), mercator.latitudeToTileY(lat, z),
      mercator.longitudeToTileLocalPixelX(lng, z), mercator.latitudeToTileLocalPixelY(lat, z), year // pass through
    );
  }

  def toPixelFeature(row: Row, z: Byte, count: Int): PixelFeature = {
    val mercator = new Mercator(4096);
    val lat = row.getDouble(row.fieldIndex("decimallatitude"));
    val lng = row.getDouble(row.fieldIndex("decimallongitude"));
    val year = row.getInt(row.fieldIndex("year"));

    return new PixelFeature(
      mercator.longitudeToTileLocalPixelX(lng, z),
      mercator.latitudeToTileLocalPixelY(lat, z),
      year, // pass through
      1
    );
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map tile processing").setMaster("local[2]")
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.parquet("/Users/tim/dev/data/map.parquet")
    df.repartition(12);



    val zoom = 14.asInstanceOf[Byte];
    val contentType = 0.asInstanceOf[Byte];
    val contentKey = "0";
    val mercator = new Mercator(4096)

    val tiles = df.flatMap(row => {
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      val year = row.getInt(row.fieldIndex("year"))

      val r1 = ((contentType,
        contentKey,
        zoom,
        mercator.longitudeToTileX(lng, zoom),
        mercator.latitudeToTileY(lat, zoom),
        mercator.longitudeToTileLocalPixelX(lng, zoom),
        mercator.latitudeToTileLocalPixelY(lat, zoom),
        year
      ),1)

      val r2 = ((contentType,
        contentKey,
        zoom,
        mercator.longitudeToTileX(lng, zoom),
        mercator.latitudeToTileY(lat, zoom),
        mercator.longitudeToTileLocalPixelX(lng, zoom),
        mercator.latitudeToTileLocalPixelY(lat, zoom),
        year
        ),1)

      List(r1,r2);

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
      .map(r => {
        // type:key:zoom:x:y as a string
        val key = r._1._1 + ":" +
                  r._1._2 + ":" +
                  r._1._3 + ":" +
                  r._1._4 + ":" +
                  r._1._5


        val k = new ImmutableBytesWritable(Bytes.toBytes(key))
        // TODO!
        val row = new KeyValue(
          Bytes.toBytes("1"),      // key
          Bytes.toBytes("cf"),     // column family
          Bytes.toBytes("col"),    // column
          Bytes.toBytes("123"))    // value
        (k,row)

      })

      /*

      val maxZoom = sc.broadcast(2);
      val countPerPixel = df.flatMap(row => {
        (1 to maxZoom.value).flatMap(zoom => {

          List(
            (toTile(row, zoom.asInstanceOf[Byte]), 1),
            (toTile(row, zoom.asInstanceOf[Byte]), 1)
          )
        })
      }).reduceByKey((x,y) => (x+y))
        .map(r => {
          val k = new ImmutableBytesWritable(Bytes.toBytes(r._1.x))
          val row = new KeyValue(
            Bytes.toBytes("1"),      // key
            Bytes.toBytes("cf"),     // column family
            Bytes.toBytes("col"),    // column
            Bytes.toBytes("123"))   // value
          (k,row)
      })

      */
    //countPerPixel.cache();


    //sqlContext.createDataFrame(countPerPixel.toJavaRDD()).show();


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
      .saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/spark7", classOf[ImmutableBytesWritable],
        classOf[Put], classOf[HFileOutputFormat], outputConf)
  }
}
