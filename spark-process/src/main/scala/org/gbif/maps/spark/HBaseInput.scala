package org.gbif.maps.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.slf4j.LoggerFactory

class Occurrence (
  val rowkey: String,

  val datasetKey: String,
  val publishingOrgKey: String,
  val publishingCountry: String,
  val countryCode: String,
  val basisOfRecord: String,

  val decimalLatitude: Double,
  val decimalLongitude: Double,
  val kingdomKey: Int,
  val phylumKey: Int,
  val classKey: Int,

  val orderKey: Int,
  val familyKey: Int,
  val genusKey: Int,
  val speciesKey: Int,
  val taxonKey: Int,

  val year: Int,
  val hasGeospatialIssues: Boolean
) extends Serializable {}

/**
  * Reads occurrence data from HBase, producing a DataFrame.
  */
object HBaseInput {
  val logger = LoggerFactory.getLogger("org.gbif.maps.spark.HBaseInput")

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    // Other options for hbase configuration are available, please check
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HConstants.html
    val zkQuorum = "c1n1.gbif.org,c1n2.gbif.org,c1n6.gbif.org"
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum)
    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    val inputTable = "dev_occurrence"

    readFromHBase(conf, sc, inputTable)

    sc.stop()
  }

  def readFromHBase(conf: Configuration, sc: SparkContext, inputTable: String) : DataFrame = {
    val sqlContext = new SQLContext(sc)

    conf.set(HConstants.HBASE_CLIENT_SCANNER_CACHING, "1000") // TODO Configure

    conf.set(TableInputFormat.INPUT_TABLE, inputTable) // TODO Configure, not parameter

    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "o")
    conf.set(TableInputFormat.SCAN_COLUMNS,
      "o:datasetKey o:publishingOrgKey o:publishingCountry o:countryCode o:basisOfRecord " +
      "o:decimalLatitude o:decimalLongitude o:kingdomKey o:phylumKey o:classKey " +
      "o:orderKey o:familyKey o:genusKey o:speciesKey o:taxonKey " +
      "o:year o:hasGeospatialIssues")

    val hBaseRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val allOccurrences = hBaseRDD.map(x => { // TODO x?
      val r = x._2 // TODO r?
      val keyRow = Bytes.toString(r.getRow())

      val o = Bytes.toBytes("o");
      val empty : Array[Byte] = new Array[Byte](4)

      // TODO Don't make an Occurrence, only to make something else straight afterwards.

      val row = new Occurrence(
        keyRow,

        Bytes.toString(Option(r.getValue(o, Bytes.toBytes("datasetKey"))).getOrElse(empty)),
        Bytes.toString(Option(r.getValue(o, Bytes.toBytes("publishingOrgKey"))).getOrElse(empty)),
        Bytes.toString(Option(r.getValue(o, Bytes.toBytes("publishingCountry"))).getOrElse(empty)),
        Bytes.toString(Option(r.getValue(o, Bytes.toBytes("countryCode"))).getOrElse(empty)),
        Bytes.toString(Option(r.getValue(o, Bytes.toBytes("basisOfRecord"))).getOrElse(empty)),

        Bytes.toDouble(Option(r.getValue(o, Bytes.toBytes("decimalLatitude"))).getOrElse(new Array[Byte](8))),
        Bytes.toDouble(Option(r.getValue(o, Bytes.toBytes("decimalLongitude"))).getOrElse(new Array[Byte](8))),
        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("kingdomKey"))).getOrElse(empty)),
        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("phylumKey"))).getOrElse(empty)),
        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("classKey"))).getOrElse(empty)),

        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("orderKey"))).getOrElse(empty)),
        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("familyKey"))).getOrElse(empty)),
        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("genusKey"))).getOrElse(empty)),
        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("speciesKey"))).getOrElse(empty)),
        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("taxonKey"))).getOrElse(empty)),

        Bytes.toInt(Option(r.getValue(o, Bytes.toBytes("year"))).getOrElse(empty)),
        Bytes.toBoolean(Option(r.getValue(o, Bytes.toBytes("hasGeospatialIssues"))).getOrElse(new Array[Byte](1)))
      )
      (keyRow, row)
    })

//    val filteredOccurrences = filteredOccurrences.filter(row => {
//      val occ = row._2
//        (occ.decimalLatitude != 0) &&
//        (occ.decimalLongitude != 0) &&
//      !occ.hasGeospatialIssues
//    })

    allOccurrences.take(20).foreach(row => logger.info("Sample: {}", row._2))

    val types : List[DataType] = List(
      StringType,
      StringType,
      StringType,
      StringType,
      StringType,

      DoubleType,
      DoubleType,
      IntegerType,
      IntegerType,
      IntegerType,

      IntegerType,
      IntegerType,
      IntegerType,
      IntegerType,
      IntegerType,

      IntegerType,
      BooleanType)

    // Creating an SQL DataFrame
    // TODO explain this!
    val fields = ("datasetKey publishingOrgKey publishingCountry countryCode basisOfRecord " +
      "decimalLatitude decimalLongitude kingdomKey phylumKey classKey " +
      "orderKey familyKey genusKey speciesKey taxonKey " +
      "year hasGeospatialIssues").toLowerCase.split(" ").zip(types).map{ case (fieldName: String, fieldType: DataType) => StructField(fieldName, fieldType, nullable = true)}
    val schema = StructType(fields)
    val rowRDD = allOccurrences
      .map(_._2)
      .map(attributes => Row(
        attributes.datasetKey,
        attributes.publishingOrgKey,
        attributes.publishingCountry,
        attributes.countryCode,
        attributes.basisOfRecord,

        attributes.decimalLatitude,
        attributes.decimalLongitude,
        attributes.kingdomKey,
        attributes.phylumKey,
        attributes.classKey,

        attributes.orderKey,
        attributes.familyKey,
        attributes.genusKey,
        attributes.speciesKey,
        attributes.taxonKey,

        attributes.year,
        attributes.hasGeospatialIssues
      ))
    val df : DataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // TODO Explain why this doesn't work.
//    val df : DataFrame = sqlContext.createDataFrame(xxx.values, classOf[Occurrence])

    allOccurrences.take(20).foreach(row => logger.info("Sample 2: {}", row._2))
    logger.error("Columns are {}", df.columns)

    return df
  }
}
