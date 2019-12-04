package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.PatchedHFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.Partitioner
import org.gbif.maps.common.projection.Tiles
import org.gbif.maps.common.hbase.ModulusSalt
import org.gbif.maps.io.PointFeature.PointFeatures.Feature
import org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}
import scala.collection.{Set, mutable}
import org.gbif.maps.tile._

/**
  * A builder of HFiles for a tile pyramid.
  */
object BackfillTiles {
  private val GEOMETRY_FACTORY = new GeometryFactory()
  private val logger = LoggerFactory.getLogger("org.gbif.maps.spark.BackfillTiles")

  /**
    * A partitioner that puts pixels in the same category together but ignores Z, X and Y.
    * This is useful so that merging across zooms remains a data local task.
    */
  class TileGroupPartitioner[K,V](partitions: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[(String,String,Feature.BasisOfRecord)]
      // skip ZXY, as that would mean we can't downscale
      var hc = (k._1 + k._3).hashCode()
      if (hc < 0) hc *= -1
      return hc%numPartitions
    }

    override def numPartitions: Int = partitions
  }

  /**
    * Build for the given projection configuration.
    *
    * @param projectionConfig Configuration of the SRS and zoom levels etc
    */
  def build(spark: SparkSession, df: DataFrame, keys: Set[String], config: MapConfiguration, projectionConfig: ProjectionConfig) = {

    val keySalter = new ModulusSalt(config.hbase.keySaltModulus); // salted HBase keys

    // TESTING Repartitioning because data skew here gets conflated quickly due to the flatMap and results in "stragglers"
    // In particular the source can be unbalanced regions from HBase since the occurrence table is not partitioned well
    //val tiles = df.repartition(config.tilePyramid.numPartitions).flatMap(row => {

    import spark.implicits._

    val tiles = df.rdd
      .filter(row => !row.isNullAt(row.fieldIndex("decimallatitude")) && !row.isNullAt(row.fieldIndex("decimallongitude"))) //has coordinates
      .flatMap(row => {

      val res = mutable.ArrayBuffer[((String, ZXY, EncodedPixel, String, Year), Int)]()
      val projection = Tiles.fromEPSG(projectionConfig.srs, projectionConfig.tileSize)

      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      if (projection.isPlottable(lat, lng)) {

        // locate the tile and pixel we are dealing with
        val zoom = projectionConfig.maxZoom.asInstanceOf[Byte]
        val globalXY = projection.toGlobalPixelXY(lat, lng, zoom) // global pixel address space
        val tileXY = Tiles.toTileXY(globalXY, projectionConfig.tileSchema, zoom, projectionConfig.tileSize) // addressed the tile
        val x = tileXY.getX
        val y = tileXY.getY
        val tileLocalXY = Tiles.toTileLocalXY(globalXY, projectionConfig.tileSchema, projectionConfig.maxZoom, x, y, projectionConfig.tileSize,
          config.tilePyramid.tileBufferSize) // pixels on the tile
        val pixel = Pixel(tileLocalXY.getX.asInstanceOf[Short], tileLocalXY.getY.asInstanceOf[Short]) // note: rounds here
        val encPixel = encodePixel(pixel)
        val zxy = ZXY(zoom, x, y)

        // read the fields of interest
        val bor: String = try {
          row.getString(row.fieldIndex("basisofrecord"))
        } catch {
          case ex: Exception => { logger.error("Unknown BasisOfRecord {}", row.getString(row.fieldIndex("basisofrecord")));  }
            "UNKNOWN"
        }

        val year: Short =
          if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Short]
          else row.getInt(row.fieldIndex("year")).asInstanceOf[Short]

        // extract the keys for the record and filter to only those that are meant to be put in a tile pyramid
        val mapKeys = MapUtils.mapKeysForRecord(row).intersect(keys)
        mapKeys.foreach(mapKey => {
          res += (((mapKey, zxy, encPixel, bor, year), 1))
        })
      }
      res
    }).reduceByKey(_+_, config.tilePyramid.numPartitions).map(r => {

      // we deferred this because enums in protobuf do not work well with Spark SQL Dataset
      // Same as: https://github.com/scalapb/ScalaPB/issues/87
      val bor: BasisOfRecord = MapUtils.BASIS_OF_RECORD(r._1._4)

      // ((type, zxy, bor), (pixel, year, count))
      ((r._1._1 : String, r._1._2 : ZXY, bor: Feature.BasisOfRecord), (r._1._3 : EncodedPixel, r._1._5 : Year, r._2 /*Count*/))
    }).partitionBy(new TileGroupPartitioner(config.tilePyramid.numPartitions))

    // Maintain the same key structure of type+zxy+bor and rewrite values into a map of "PixelYear" → count
    val appendVal = { (m: MMap[Long,Int], v: (Int,Short,Int)) =>
      val py = encodePixelYear(v._1, v._2)
      m += ((py, v._3 + m.getOrElse(py,0)))
    }
    val merge = { (m1: MMap[Long,Int], m2: MMap[Long,Int]) =>
      // merge maps accumulating the counts
      m1 ++ m2.map{ case (k,v) => k -> (v + m1.getOrElse(k,0)) }
    }
    val tiles2 = tiles.aggregateByKey(MMap[Long,Int]().empty)(appendVal, merge)

    type TileFeatures = (ZXY, BasisOfRecord, MMap[Long,Int])
    val createTile = (pixels: TileFeatures) => {
      val (zxy, basisOfRecord, features) = pixels
      val tile = new OccurrenceDensityTile(zxy, projectionConfig.tileSize, config.tilePyramid.tileBufferSize)
      tile.collect(basisOfRecord, features)
      tile
    }

    val featureCollector = (collector: OccurrenceDensityTile, pixels: TileFeatures) => {
      val (zxy, basisOfRecord, features) = pixels
      collector.collect(basisOfRecord, features)
    }

    val tileMerger = (t1: OccurrenceDensityTile, t2: OccurrenceDensityTile) => OccurrenceDensityTile.merge(t1,t2)

    // Re-key the data into type+zxy and create the density tile
    var tiles3 = tiles2.map(r => {
      // (type, zxy) , (zxy, bor, (pixel+year -> count))
      ((r._1._1,r._1._2.toString),(r._1._2, r._1._3, r._2))
    }).combineByKey(createTile, featureCollector, tileMerger)

    /**
      * For each zoom level working from max to min, we merge from the previous zoom (if required), then build
      * a buffer zone around each tile pulling in data from adjacent tiles, and then encode the data as vector tile
      * format, and finally produce HFiles.
      */
    var downscale = false;
    (projectionConfig.minZoom to projectionConfig.maxZoom).reverse.foreach(z => {
      spark.sparkContext.setLocalProperty("callSite.short", "BackfillTiles: "+projectionConfig.srs+" z"+z+"/"+projectionConfig.maxZoom)

      //val downscale = z < projectionConfig.maxZoom

      tiles3 = tiles3.flatMap(t => {
        val result = mutable.ArrayBuffer[((String, String), OccurrenceDensityTile)]()
        val mapKey: String = t._1._1
        val tiles = t._2.flatMapToBuffers(projectionConfig.tileSchema, downscale)
        for (tile <- tiles) {
          result += (((mapKey, tile.getZXY().toString), tile.asInstanceOf[OccurrenceDensityTile]))
        }
        result
      }).reduceByKey(tileMerger)

      /**
        * Generate the vector tile and write it as an HFile.
        */
      val tiles4 = tiles3.mapValues(tile => {
        // set up the encoder with no buffer and false to indicate that the features are not 0..255 space, but
        // already in the the space of the tileSize
        val bufferSize = config.tilePyramid.tileBufferSize
        val encoder = new VectorTileEncoder(projectionConfig.tileSize, bufferSize, false)

        for ((basisOfRecord, features) <- tile.getData()) {
          // unpack the encoded pixel years and create a map with pixels and the metadata
          val pixels = MMap[Pixel, MMap[Short,Int]]()

          for ((pixel, yearCount) <- features.iterator()) {
            if (pixels.contains(pixel)) {
              val pixelMeta = pixels(pixel)
              pixelMeta.update(yearCount.year, pixelMeta.getOrElse(yearCount.year, 0) + yearCount.count)
            } else {
              val pixelMeta = MMap[Short,Int]()
              pixelMeta += (yearCount.year -> yearCount.count)
              pixels.put(pixel, pixelMeta)
            }
          }

          for ((pixel, meta) <- pixels) {
            val point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.x.toDouble, pixel.y.toDouble));
            // VectorTiles want String:Object format
            val metaAsString = new java.util.HashMap[String, Any]()

            // TODO: move this type conversion to above
            for ((year, count) <- meta) {
              metaAsString.put(String.valueOf(year), count)
            }
            encoder.addFeature(basisOfRecord.toString, metaAsString, point);
          }
        }
        encoder.encode()

      })

      val tiles5 = tiles4.map(r => {
        // mapType:mapKey:z:x:y for a tall, narrow table
        val rowKey = r._1._1 + ":" + r._1._2
        val saltedRowKey = keySalter.saltToString(rowKey)
        (saltedRowKey,r._2)
      }).repartitionAndSortWithinPartitions(new MapUtils.SaltPrefixPartitioner(keySalter.saltCharCount()))

      tiles5.map(r => {
        val saltedRowKey = Bytes.toBytes(r._1)
        val k = new ImmutableBytesWritable(saltedRowKey)
        val cellData = r._2

        val row = new KeyValue(saltedRowKey, // key
          Bytes.toBytes(projectionConfig.srs.replaceAll(":", "_")), // column family (e.g. epsg_4326)
          Bytes.toBytes("tile"),
          cellData)
        (k, row)
      }).saveAsNewAPIHadoopFile(
        config.targetDirectory + "/tiles/" + projectionConfig.srs.replaceAll(":", "_") + "/z" + z,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[PatchedHFileOutputFormat2],
        Configurations.hfileOutputConfiguration(config, config.tilePyramid.tableName))

      downscale = true; // TODO: such as hack!
    }) // zoom
  }
}
