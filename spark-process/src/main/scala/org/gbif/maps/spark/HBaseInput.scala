package org.gbif.maps.spark

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormatImpl}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.slf4j.LoggerFactory

/**
  * Reads occurrence data from an HBase snapshot producing a DataFrame.
  */
object HBaseInput {
  val logger = LoggerFactory.getLogger("org.gbif.maps.spark.HBaseInput")

  def readFromHBase(config: MapConfiguration, sc: SparkContext) : DataFrame = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, config.hbase.zkQuorum)
    hbaseConf.set("hbase.rootdir", config.hbase.rootDir)

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

      ("_iss_ZERO_COORDINATE", BooleanType),
      ("_iss_COORDINATE_INVALID", BooleanType),
      ("_iss_COORDINATE_OUT_OF_RANGE", BooleanType),
      ("_iss_COUNTRY_COORDINATE_MISMATCH", BooleanType)

    )

    // Set up the scan to only be concerned about the columns of interest
    val scan = new Scan
    scan.setCaching(10000);
    val cf = Bytes.toBytes("o")
    fieldNamesAndTypes.foreach(ft => {
      scan.addColumn(cf, Bytes.toBytes(ft._1))
    })
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val job = Job.getInstance(hbaseConf)
    TableSnapshotInputFormat.setInput(job, config.source, new Path(config.hbase.restoreDir))

    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // Creating an SQL DataFrame
    val fields = fieldNamesAndTypes.map{ case (fieldName, fieldType) => StructField(fieldName.toLowerCase(), fieldType, nullable = true) }
    val schema = StructType(fields)

    val o = Bytes.toBytes(columnFamily)
    val empty = new Array[Byte](4)
    val emptyBoolean = new Array[Byte](1)


    val rowRDD = hBaseRDD.filter{ case(_, result) => {

      // Filter out records without a position, or with a geospatial issue (any value means issue)
      // see https://github.com/gbif/gbif-api/blob/master/src/main/java/org/gbif/api/vocabulary/OccurrenceIssue.java#L377
      Option(result.getValue(o, Bytes.toBytes("decimalLatitude"))).nonEmpty &&
      Option(result.getValue(o, Bytes.toBytes("decimalLongitude"))).nonEmpty &&
      Option(result.getValue(o, Bytes.toBytes("_iss_ZERO_COORDINATE"))).isEmpty &&
      Option(result.getValue(o, Bytes.toBytes("_iss_COORDINATE_INVALID"))).isEmpty &&
      Option(result.getValue(o, Bytes.toBytes("_iss_COORDINATE_OUT_OF_RANGE"))).isEmpty &&
      Option(result.getValue(o, Bytes.toBytes("_iss_COUNTRY_COORDINATE_MISMATCH"))).isEmpty

    }}.map{ case(_, result) => {
      Row(
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("datasetKey"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("publishingOrgKey"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("publishingCountry"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("countryCode"))).getOrElse(empty)),
        Bytes.toString(Option(result.getValue(o, Bytes.toBytes("basisOfRecord"))).getOrElse(empty)),

        Bytes.toDouble(result.getValue(o, Bytes.toBytes("decimalLatitude"))), // Filter ensures not null.
        Bytes.toDouble(result.getValue(o, Bytes.toBytes("decimalLongitude"))),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("kingdomKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("phylumKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("classKey"))).getOrElse(empty)),

        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("orderKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("familyKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("genusKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("speciesKey"))).getOrElse(empty)),
        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("taxonKey"))).getOrElse(empty)),

        Bytes.toInt(Option(result.getValue(o, Bytes.toBytes("year"))).getOrElse(empty)),
        Option(result.getValue(o, Bytes.toBytes("_iss_ZERO_COORDINATE"))).isEmpty,
        Option(result.getValue(o, Bytes.toBytes("_iss_COORDINATE_INVALID"))).isEmpty,
        Option(result.getValue(o, Bytes.toBytes("_iss_COORDINATE_OUT_OF_RANGE"))).isEmpty,
        Option(result.getValue(o, Bytes.toBytes("_iss_COUNTRY_COORDINATE_MISMATCH"))).isEmpty
      )
    }}

    sqlContext.createDataFrame(rowRDD, schema)
  }

  /**
    * Copied from TableMapReduceUtil
    */
  def convertScanToString(scan : Scan) = {
    val proto = ProtobufUtil.toScan(scan);
    Base64.encodeBytes(proto.toByteArray());
  }
}
