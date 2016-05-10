import scala.collection.mutable.Map;

/**
  * Utilities for dealing with maps.
  * This could surely be more generic, but that is beyond the current skill of this author.
  */
object Maps {
  /**
    * Merges map1 and map2 into a new immutable(!) map.
    * <ol>
    *   <li>If map2 holds a key not in map1 the entry is added to map1</li>
    *   <li>If map2 holds a key that is in map1 then the inner map is merged, accumulating any entries keyed in both maps.
    * </ol>
    */
  def merge(map1 : Map[(Int,Int), Map[Int,Int]], map2 : Map[(Int,Int), Map[Int,Int]]) = (map1.keySet ++ map2.keySet)
    .map(key => key -> mergeValues(map1.get(key), map2.get(key)))
    .toMap

  // merges the values if required, or selecting the only one if only one side has value
  private def mergeValues(o1 : Option[Map[Int,Int]], o2 : Option[Map[Int,Int]]) = (o1, o2) match {
    case (Some(v1 : Map[Int,Int]), Some(v2 : Map[Int,Int])) => Map() ++ merge2(v1,v2) // return mutable(!)
    case _ => (o1 orElse o2).get
  }

  // the inner map merge (same as the outer)
  private def merge2(map1 : Map[Int,Int], map2 : Map[Int,Int]) = (map1.keySet ++ map2.keySet)
    .map(key => key -> mergeValues2(map1.get(key), map2.get(key)))
    .toMap

  private def mergeValues2(o1 : Option[Int], o2 : Option[Int]) = (o1, o2) match {
    case (Some(v1: Int), Some(v2: Int)) => v1 + v2 // accumulate if required
    case _ => (o1 orElse o2).get
  }
}


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

val df = sqlContext.read.parquet("/user/hive/warehouse/tim.db/occurrence_map_source").repartition(500)
    
    
    val POINT_THRESHOLD = 100000;
    val MERCATOR = new Mercator(4096)
    val GEOMETRY_FACTORY = new GeometryFactory()
    val MAX_HFILES_PER_CF_PER_REGION = 32 // defined in HBase's LoadIncrementalHFiles
    val MAX_ZOOM = 4;

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
      //if (!row.isNullAt(row.fieldIndex("kingdomkey"))) taxonIDs+=row.getInt(row.fieldIndex("kingdomkey"))
      //if (!row.isNullAt(row.fieldIndex("phylumkey"))) taxonIDs+=row.getInt(row.fieldIndex("phylumkey"))
      //if (!row.isNullAt(row.fieldIndex("classkey"))) taxonIDs+=row.getInt(row.fieldIndex("classkey"))
      if (!row.isNullAt(row.fieldIndex("orderkey"))) taxonIDs+=row.getInt(row.fieldIndex("orderkey"))
      if (!row.isNullAt(row.fieldIndex("familykey"))) taxonIDs+=row.getInt(row.fieldIndex("familykey"))
      if (!row.isNullAt(row.fieldIndex("genuskey"))) taxonIDs+=row.getInt(row.fieldIndex("genuskey"))
      if (!row.isNullAt(row.fieldIndex("specieskey"))) taxonIDs+=row.getInt(row.fieldIndex("specieskey"))
      if (!row.isNullAt(row.fieldIndex("taxonkey"))) taxonIDs+=row.getInt(row.fieldIndex("taxonkey"))

      val m1 = mutable.ArrayBuffer[((Int,Any,Double,Double,Int,String),Int)](
        //((MAPS_TYPES("ALL"), 0, lat, lng, year, basisOfRecord),1),
        ((MAPS_TYPES("DATASET"), datasetKey, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("PUBLISHER"), publisherKey, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("COUNTRY"), country, lat, lng, year, basisOfRecord), 1),
        ((MAPS_TYPES("PUBLISHING_COUNTRY"), publishingCountry, lat, lng, year, basisOfRecord), 1)        
      )

      // add the distinct taxa
      taxonIDs.foreach(id => {
        m1 += (((MAPS_TYPES("TAXON"), id, lat, lng, year, basisOfRecord), 1))
      })

      m1
    }).reduceByKey(_ + _)
    
    
    // take the First N records from each group and collect them suitable for HFile writing
    val pointData = initialCounts.map(r => {
      ((r._1._1 + ":" + r._1._2), (r._1._3, r._1._4, r._1._5, r._1._6))

    }).groupByKey().map(r => {
      (r._1, r._2.take(POINT_THRESHOLD))
    }).mapValues(r => {
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
    }).repartitionAndSortWithinPartitions(new HashPartitioner(MAX_HFILES_PER_CF_PER_REGION)).map(r => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(r._1))
      val row = new KeyValue(Bytes.toBytes(r._1), // key
        Bytes.toBytes("wgs84"), // column family
        Bytes.toBytes("features"), // cell
        r._2 // cell value
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

    pointData.saveAsNewAPIHadoopFile("hdfs://c1n1.gbif.org:8020/tmp/zp27", classOf[ImmutableBytesWritable],
        classOf[KeyValue], classOf[HFileOutputFormat], outputConf)

    
