package org.gbif.maps.spark

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.gbif.maps.common.projection.Mercator;


/**
  * Processes tiles.
  */
object AppWorking {

  case class Pixel(val x: Long, val y: Long, val px: Int, val py: Int, val year: Int);

  def toTile(row: Row, z: Byte): Pixel = {
    val mercator = new Mercator(4096);
    val lat = row.getDouble(row.fieldIndex("decimallatitude"));
    val lng = row.getDouble(row.fieldIndex("decimallongitude"));
    val year = row.getInt(row.fieldIndex("year"));

    return new Pixel(mercator.longitudeToTileX(lng, z), mercator.latitudeToTileY(lat, z),
      mercator.longitudeToTileLocalPixelX(lng, z), mercator.latitudeToTileLocalPixelY(lat, z), year // pass through
    );

  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Map tile processing").setMaster("local[2]")
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.parquet("/Users/tim/dev/data/map.parquet")
    df.repartition(12);


    val maxZoom = sc.broadcast(2);

    val k1 = new ImmutableBytesWritable(Bytes.toBytes(1))
    print(k1)

    /*
    // example: use zoom level 2
    val zoom = 2.asInstanceOf[Byte];

    // Count coordinates by pixel on each tile
    val countPerPixel = df.map(row => {
      (toTile(row, zoom), 1)
    }).reduceByKey((a, b) => a + b)
    */

    val countPerPixel = df.flatMap(row => {
      (1 to maxZoom.value).map(zoom => {
        (toTile(row, zoom.asInstanceOf[Byte]), 1)
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

    countPerPixel
      .saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/spark", classOf[ImmutableBytesWritable],
        classOf[Put], classOf[HFileOutputFormat], outputConf)
  }
}
