package org.gbif.maps.spark

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.gbif.maps.common.hbase.ModulusSalt
import org.gbif.maps.io.PointFeature
import org.gbif.maps.io.PointFeature.PointFeatures.Feature
import org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}

import scala.collection.mutable
import scala.collection.Set

/**
  * The workflow for backfilling the points.
  *
  * The output will be a collection of HFiles in the "points-wgs84" subdirectory within the target directory.
  * The HFiles are destined for a single column family "wgs84" and there is a single cell of "features" for each row
  * which keyed as an encoded mapKey (i.e. mapType:key).  The "features" cell will contain a protobuf encoding a
  * collection of objects, each representing the count of records at a single location+year+basisOfRecord combination.
  */
object BackfillPoints {
  val logger = LoggerFactory.getLogger("org.gbif.maps.spark.BackfillPoints")

  def build(sc :SparkContext, df : DataFrame, keys: Set[String], config: MapConfiguration): Unit = {

    val keySalter = new ModulusSalt(config.hbase.keySaltModulus); // salted HBase keys

    val pointSource = df.flatMap(row => {
      // extract the keys for the record and filter to only those that have been determined as suitable
      val mapKeys = MapUtils.mapKeysForRecord(row).intersect(keys)

      // extract the dimensions of interest from the record
      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      val bor: BasisOfRecord = try {
        MapUtils.BASIS_OF_RECORD(row.getString(row.fieldIndex("basisofrecord")))
      } catch {
        case ex: Exception => { logger.error("Unknown BasisOfRecord {}", row.getString(row.fieldIndex("basisofrecord")));  }
          PointFeature.PointFeatures.Feature.BasisOfRecord.UNKNOWN
      }

      val year = if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Short]
      else row.getInt((row.fieldIndex("year"))).asInstanceOf[Short]

      // Stuctured as: mapKey, latitude, longitude, basisOfRecord, year -> count
      val res = mutable.ArrayBuffer[((String, Double, Double, Feature.BasisOfRecord, Short), Long)]()
      mapKeys.foreach(mapKey => {
          res += (((mapKey, lat, lng, bor, year),1))
      })

      res
    }).reduceByKey(_+_, config.pointFeatures.numTasks).map(r => {
      // Structured as: mapKey -> lat,lng,bor,year,count
      // NOTE: If we have more than an INT at a single location in one year something in the data is wrong

      val rowKey = r._1._1
      val saltedRowKey = keySalter.saltToString(rowKey)

      (saltedRowKey,(r._1._2,r._1._3,r._1._4,r._1._5,r._2.asInstanceOf[Int]))
    })

    // Convert into PBF and prepare to write into HFiles
    val res = pointSource.groupByKey().mapValues(r => {
      val builder = PointFeature.PointFeatures.newBuilder();
      r.foreach(f => {
        val fb = PointFeature.PointFeatures.Feature.newBuilder();
        fb.setLatitude(f._1)
        fb.setLongitude(f._2)
        fb.setBasisOfRecord(f._3)
        fb.setYear(f._4)
        fb.setCount(f._5)
        builder.addFeatures(fb)
      })
      builder.build().toByteArray
    }).repartitionAndSortWithinPartitions(new MapUtils.SaltPrefixPartitioner(keySalter.saltCharCount())).map(r => {

      val saltedRowKey = Bytes.toBytes(r._1)
      val k = new ImmutableBytesWritable(saltedRowKey)
      val row = new KeyValue(saltedRowKey, // key
        Bytes.toBytes("EPSG_4326"), // column family
        Bytes.toBytes("features"), // cell
        r._2 // cell value
      )
      (k, row)
    })

    // save the results
    res.saveAsNewAPIHadoopFile(config.targetDirectory + "/points", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], Configurations.hfileOutputConfiguration(config, config.pointFeatures.tableName))

  }
}
