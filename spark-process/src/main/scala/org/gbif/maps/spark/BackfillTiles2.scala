package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.gbif.maps.common.projection.Tiles
import org.gbif.maps.io.PointFeature
import org.gbif.maps.io.PointFeature.PointFeatures.Feature
import org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}
import scala.collection.{Set, mutable}

/**
  * A builder of HFiles for a tile pyramid.
  */
object BackfillTiles2 {
  private val GEOMETRY_FACTORY = new GeometryFactory()
  private val logger = LoggerFactory.getLogger("org.gbif.maps.spark.BackfillTiles2")

  /**
    * A partitioner that puts pixels in the same category together but ignores Z,X and Y.
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
  def build(sc: SparkContext, df: DataFrame, keys: Set[String], config: MapConfiguration, projectionConfig: ProjectionConfig) = {

    val tiles = df.flatMap(row => {
      val res = mutable.ArrayBuffer[((String, String, EncodedPixel, Feature.BasisOfRecord, Year), Int)]()
      val projection = Tiles.fromEPSG(projectionConfig.srs, projectionConfig.tileSize)

      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      if (projection.isPlottable(lat, lng)) {

        // locate the tile and pixel we are dealing with
        val zoom = projectionConfig.maxZoom.asInstanceOf[Byte]
        val globalXY = projection.toGlobalPixelXY(lat, lng, zoom) // global pixel address space
        val tileXY = Tiles.toTileXY(globalXY, zoom, projectionConfig.tileSize) // addressed the tile
        val x = tileXY.getX
        val y = tileXY.getY
        val tileLocalXY = Tiles.toTileLocalXY(globalXY, x, y, projectionConfig.tileSize) // pixels on the tile
        val pixel = Pixel(tileLocalXY.getX.asInstanceOf[Short], tileLocalXY.getY.asInstanceOf[Short]) // note: rounds here
        val encPixel = MapUtils.encodePixel(pixel)
        val zxy = MapUtils.toZXY(zoom, x, y) // the encoded tile address

        // read the fields of interest
        val bor: BasisOfRecord = try {
          MapUtils.BASIS_OF_RECORD(row.getString(row.fieldIndex("basisofrecord")))
        } catch {
          case ex: Exception => { logger.error("Unknown BasisOfRecord {}", row.getString(row.fieldIndex("basisofrecord"))); }
            PointFeature.PointFeatures.Feature.BasisOfRecord.UNKNOWN
        }
        val year =
          if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Short]
          else row.getInt((row.fieldIndex("year"))).asInstanceOf[Short]

        // extract the keys for the record and filter to only those that are meant to be put in a tile pyramid
        val mapKeys = MapUtils.mapKeysForRecord(row).intersect(keys)
        mapKeys.foreach(mapKey => {
          res += (((mapKey, zxy, encPixel, bor, year), 1))
        })
      }
      res
    }).reduceByKey(_+_, config.tilePyramid.numPartitions).map(r => {
      // ((type, zxy, bor), (pixel, year, count))
      ((r._1._1 : String, r._1._2 : String, r._1._4 : Feature.BasisOfRecord), (r._1._3 : EncodedPixel, r._1._5 : Year, r._2 /*Count*/))
    }).partitionBy(new TileGroupPartitioner(config.tilePyramid.numPartitions))

    // Maintain the same key structure of type+zxy+bor and rewrite values into a map of "PixelYear" → count
    val appendVal = { (m: MMap[Long,Int], v: (Int,Short,Int)) =>
      val py = MapUtils.encodePixelYear(v._1, v._2)
      m += ((py, v._3))
    }
    val merge = { (m1: MMap[Long,Int], m2: MMap[Long,Int]) =>
      m1 ++= m2
    }
    val tiles2 = tiles.aggregateByKey(MMap[Long,Int]().empty)(appendVal, merge)

    // Re-key the data into type+zxy and create the density tile
    var tiles3 = tiles2.map(r => {
      // type, zxy -> bor : (pixel+year -> count)
      ((r._1._1,r._1._2),(r._1._3, r._2))
    }).aggregateByKey(new DensityTile())((tile, v) => tile.collect(v._1, v._2), DensityTile.merge(_,_))

    /**
      * For each zoom level working from max to min, we merge from the previous zoom (if required), then build
      * a buffer zone around each tile pulling in data from adjacent tiles, and then encode the data as vector tile
      * format, and finally produce HFiles.
      */
    (projectionConfig.minZoom to projectionConfig.maxZoom).reverse.foreach(z => {

      // downscale if needed, by merging the previous zoom tiles together (each quad of tiles become one tile)
      if (z != projectionConfig.maxZoom) {

        tiles3 = tiles3.flatMap(t => {
          val result = mutable.ArrayBuffer[((String, String), DensityTile)]()

          val mapKey = t._1._1
          val (_, x, y) = MapUtils.fromZXY(t._1._2)
          val namedTiles = t._2.downscaleBuffer(x, y, projectionConfig.tileSize, config.tilePyramid.tileBufferSize, true)

          // rewrite the X and Y which half each time you zoom out
          val newTileX = x/2
          val newTileY = y/2

          namedTiles.map {
            case(TileBuffers.Region.CENTER, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX,newTileY)),tile))
            case(TileBuffers.Region.N, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX,newTileY-1)),tile))
            case(TileBuffers.Region.NE, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX+1,newTileY-1)),tile))
            case(TileBuffers.Region.E, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX+1,newTileY)),tile))
            case(TileBuffers.Region.SE, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX+1,newTileY+1)),tile))
            case(TileBuffers.Region.S, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX,newTileY+1)),tile))
            case(TileBuffers.Region.SW, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX-1,newTileY+1)),tile))
            case(TileBuffers.Region.W, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX-1,newTileY)),tile))
            case(TileBuffers.Region.NW, tile) => result += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],newTileX-1,newTileY-1)),tile))
          }

          result
        }).reduceByKey(DensityTile.merge(_,_))
      }

      /**
        * Generate the vector tile and write it as an HFile.
        */
      tiles3.mapValues(tile => {
        // set up the encoder with no buffer and false to indicate that the features are not 0..255 space, but
        // already in the the space of the tileSize
        val bufferSize = config.tilePyramid.tileBufferSize
        val encoder = new VectorTileEncoder(projectionConfig.tileSize, bufferSize, false)

        tile.getData().keySet.foreach(bor => {
          val pixelYears = tile.getData().get(bor)

          // unpack the encoded pixel years and create a map with pixels and the metadata
          var pixels = MMap[Int, MMap[Short,Int]]();
          pixelYears.get.foreach(p => {
            val py = MapUtils.decodePixelYear(p._1)
            val year = py._2
            val count = p._2
            if (pixels.contains(py._1)) {
              var pixelMeta = pixels.get(py._1).get
              pixelMeta.update(year, pixelMeta.getOrElse(year, 0) + count)
            } else {
              val pixelMeta = MMap[Short,Int]()
              pixelMeta += (year -> count)
              pixels.put(py._1, pixelMeta)
            }
          })

          pixels.foreach(p => {
            val pixel = MapUtils.decodePixel(p._1)
            val point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.x.toDouble, pixel.y.toDouble));
            // VectorTiles want String:Object format
            val meta = new java.util.HashMap[String, Any]()
            p._2.foreach(yearCount => {
              meta.put(String.valueOf(yearCount._1), yearCount._2)
            })
            encoder.addFeature(bor.toString(), meta, point);
          })
        })

        encoder.encode()

  }).repartitionAndSortWithinPartitions(new HashPartitioner(config.tilePyramid.hfileCount)).map( r => {
    val k = new ImmutableBytesWritable(Bytes.toBytes(r._1._1))
    val cell = r._1._2
    val cellData = r._2
    val row = new KeyValue(Bytes.toBytes(r._1._1), // key
      Bytes.toBytes(projectionConfig.srs.replaceAll(":", "_")), // column family (e.g. epsg_4326)
      Bytes.toBytes(cell), // cell
      cellData)
    (k, row)
  }).saveAsNewAPIHadoopFile(config.targetDirectory + "/tiles/" + projectionConfig.srs.replaceAll(":", "_") + "/z" + z, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], Configurations.hfileOutputConfiguration(config, config.tilePyramid.tableName))
    })
  }

}
