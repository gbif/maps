package org.gbif.maps.spark

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.slf4j.LoggerFactory

/**
  * Reads occurrence data from HBase, producing a DataFrame.
  */
object HBaseInput {
  val logger = LoggerFactory.getLogger("org.gbif.maps.spark.HBaseInput")

  def readFromHBase(config: MapConfiguration, sc: SparkContext) : DataFrame = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, config.hbase.zkQuorum)
    hbaseConf.set(HConstants.HBASE_CLIENT_SCANNER_CACHING, config.hbase.scannerCaching)

    val sqlContext = new SQLContext(sc)

    // Fields to read from HBase, and their types as they will be in the DataFrame
    val columnFamily = "o"
    val fieldNamesAndTypes : List[(String, DataType)] = List(
      ("datasetKey", StringType),
      ("publishingOrgKey", StringType),
      ("publishingCountry", StringType),
      ("countryCode", StringType),
      ("basisOfRecord", StringType),

      ("decimalLatitude", DoubleType),
      ("decimalLongitude", DoubleType),
      ("kingdomKey", IntegerType),
      ("phylumKey", IntegerType),
      ("classKey", IntegerType),

      ("orderKey", IntegerType),
      ("familyKey", IntegerType),
      ("genusKey", IntegerType),
      ("speciesKey", IntegerType),
      ("taxonKey", IntegerType),

      ("year", IntegerType),
      ("hasGeospatialIssues", BooleanType)
    )

    val scanColumns = fieldNamesAndTypes.map(_._1).foldLeft("")( (a, b) => a + columnFamily+":"+b+" " ).trim
    hbaseConf.set(TableInputFormat.INPUT_TABLE, config.source)
    hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, columnFamily)
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, scanColumns)

    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

//    val filteredOccurrences = filteredOccurrences.filter(row => {
//      val occ = row._2
//        (occ.decimalLatitude != 0) &&
//        (occ.decimalLongitude != 0) &&
//      !occ.hasGeospatialIssues
//    })

    // Creating an SQL DataFrame
    val fields = fieldNamesAndTypes.map{ case (fieldName, fieldType) => StructField(fieldName.toLowerCase(), fieldType, nullable = true) }
    val schema = StructType(fields)

    val empty = new Array[Byte](4)
    val emptyDouble = new Array[Byte](8)
    val emptyBoolean = new Array[Byte](1)

    val rowRDD = hBaseRDD.map{ case(_, result) => {
      val o = Bytes.toBytes(columnFamily)

      Row(
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("datasetKey"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("publishingOrgKey"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("publishingCountry"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("countryCode"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("basisOfRecord"))).getOrElse(empty)),

        Bytes.toDouble(Option(result.getValue(o, Bytes.toBytes("decimalLatitude"))).getOrElse(emptyDouble)),
        Bytes.toDouble(Option(result.getValue(o, Bytes.toBytes("decimalLongitude"))).getOrElse(emptyDouble)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("kingdomKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("phylumKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("classKey"))).getOrElse(empty)),

        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("orderKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("familyKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("genusKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("speciesKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("taxonKey"))).getOrElse(empty)),

        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("year"))).getOrElse(empty)),
        Bytes.toBoolean(Option(result.getValue(o, Bytes.toBytes("hasGeospatialIssues"))).getOrElse(emptyBoolean))
      )
    }}

    sqlContext.createDataFrame(rowRDD, schema)
  }
}
