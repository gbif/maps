package org.gbif.maps.spark

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.gbif.maps.io.PointFeature;


/**
  * Processes tiles.
  */
object AppPoints2 {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Map tile processing")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Read the source data
    val df = sqlContext.read.parquet("/Users/tim/dev/data/all_point1_deg").repartition(12)

    // Number of points to put into the single cell in HBase.
    // Maps with more features than this will be pushed into a tile pyramid
    val pointThreshold = 1000000;

    // Dictionary of type to code
    // TODO: Perhaps a Java API?
    val MAPS_TYPES = Map(
      "ALL" -> 0,
      "TAXON" -> 1,
      "DATASET" -> 2,
      "PUBLISHER" -> 3,
      "COUNTRY" -> 4,
      "PUBLISHING_COUNTRY" -> 5
    )
    val BASIS_OF_RECORD = Map(
      "UNKNOWN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.UNKNOWN,
      "PRESERVED_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.PRESERVED_SPECIMEN,
      "FOSSIL_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.FOSSIL_SPECIMEN,
      "LIVING_SPECIMEN" -> PointFeature.PointFeatures.Feature.BasisOfRecord.LIVING_SPECIMEN,
      "OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.OBSERVATION,
      "HUMAN_OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.HUMAN_OBSERVATION,
      "MACHINE_OBSERVATION" -> PointFeature.PointFeatures.Feature.BasisOfRecord.MACHINE_OBSERVATION,
      "MATERIAL_SAMPLE" -> PointFeature.PointFeatures.Feature.BasisOfRecord.MATERIAL_SAMPLE,
      "LITERATURE" -> PointFeature.PointFeatures.Feature.BasisOfRecord.LITERATURE
    )

    val initialCounts = df.flatMap(row => {
      // note that everything is lowercase (important)
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      val year = row.getInt(row.fieldIndex("year"))
      val basisOfRecord = row.getString(row.fieldIndex("basisofrecord"))

      List(
        ((MAPS_TYPES("ALL"), 0, lat, lng, year, basisOfRecord),1)
      )
    })
    .reduceByKey((x,y) => (x+y))
    .map(r => {
      ((r._1._1, r._1._2),(r._1._3, r._1._4, r._1._5, r._1._6))
    })
    .groupByKey()
    .map(r => {
      (r._1, r._2.take(pointThreshold))
    })
    .map(r => {
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
      (key,builder.build().toByteArray)
     })
    .repartition(6)
    .sortByKey(true)
    .map(r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1))
      val row = new KeyValue(
        Bytes.toBytes(r._1),      // key
        Bytes.toBytes("test"),    // column family
        Bytes.toBytes("test"),    // cell
        r._2                      // cell value
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
      val table = new HTable(conf, "tim_test")
      HFileOutputFormat.configureIncrementalLoad(job, table);
      conf
    }

    initialCounts
      .saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/tim_test_v18", classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat], outputConf)
  }
}
