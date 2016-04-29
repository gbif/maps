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
object SmallTestCluster {

  object VectorTileEncoderFactory {
    def newEncoder(): VectorTileEncoder = {
      new VectorTileEncoder(4066, 0, true)
    }
  }

  def main(args: Array[String]) {
    // skip the next 3 lines in shell
    val conf = new SparkConf().setAppName("Map tile processing").setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Read the source data
    val df = sqlContext.read.parquet("/user/hive/warehouse/tim.db/occurrence_map_source_sample")
    df.repartition(100)
;
    val test = df.map(row => {
      val speciesKey = row.getInt(row.fieldIndex("specieskey"))
      val e = new VectorTileEncoder(4096, 0, false)
      ((speciesKey), e.encode())
    });
    test.count();



  }
}
