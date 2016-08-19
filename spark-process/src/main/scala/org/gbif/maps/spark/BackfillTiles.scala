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
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{Set=> MSet}
import scala.collection.{Set, mutable}

/**
  * A builder of HFiles for a tile pyramid.
  */
object BackfillTiles {
  private val GEOMETRY_FACTORY = new GeometryFactory()

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
    * @param projectionConfig Configuration of the SRS and zoom levels etc
    */
  def build(sc :SparkContext, df : DataFrame, keys: Set[String], config: MapConfiguration, projectionConfig: ProjectionConfig) = {

    var tiles = df.flatMap(row => {
      val res = mutable.ArrayBuffer[((String, String, Int, Feature.BasisOfRecord, Short),Int)]()
      val projection = Tiles.fromEPSG(projectionConfig.srs, projectionConfig.tileSize)

      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))
      if (projection.isPlottable(lat, lng)) {

        // locate the tile and pixel we are dealing with
        val z = projectionConfig.maxZoom.asInstanceOf[Byte] // zoom
        val globalXY = projection.toGlobalPixelXY(lat,lng,z) // global pixel address space
        val tileXY = Tiles.toTileXY(globalXY, z, projectionConfig.tileSize) // addressed the tile
        val x = tileXY.getX
        val y = tileXY.getY
        val tileLocalXY = Tiles.toTileLocalXY(globalXY, x, y, projectionConfig.tileSize) // pixels on the tile
        val px = tileLocalXY.getX.asInstanceOf[Short] // note: rounds here
        val py = tileLocalXY.getY.asInstanceOf[Short] // note: rounds here
        val pixel = MapUtils.encodePixel(px, py)
        val zxy = MapUtils.toZXY(z,x,y) // the encoded tile address

        // read the fields of interest
        val bor = MapUtils.BASIS_OF_RECORD(row.getString(row.fieldIndex("basisofrecord")))
        val year = if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Short]
        else row.getInt((row.fieldIndex("year"))).asInstanceOf[Short]

        // extract the keys for the record and filter to only those that are meant to be put in a tile pyramid
        val mapKeys = MapUtils.mapKeysForRecord(row).intersect(keys)
        mapKeys.foreach(mapKey => {
          res += (((mapKey, zxy, pixel, bor, year), 1))
        })
      }
      res
    }).reduceByKey(_+_, 200).map(r => {       // TODO: configurify this as a "numPartitions" property
      // type, zxy, bor : pixel,year,count
      ((r._1._1, r._1._2, r._1._4),(r._1._3,r._1._5,r._2))
    }).partitionBy(new TileGroupPartitioner(200))   // TODO: use "numPartitions" property

    // Maintain the same key structure of type+zxy+bor and rewrite values into a map of "pixelYear" -> count
    val appendVal = (m: MMap[Long,Int], v: (Int,Short,Int)) => {
      val py = MapUtils.encodePixelYear(v._1,v._2)
      m+=((py, v._3))
    }
    val merge = (m1: MMap[Long,Int], m2: MMap[Long,Int]) => {
      m1 ++= m2
    }
    val tiles2 = tiles.aggregateByKey(MMap[Long,Int]().empty)(appendVal, merge)

    // Re-key the data into type+zxy and move the bor into the value as a map of basisOfRecord -> "pixelYear" -> count
    val appendVal2 = (m: MMap[Feature.BasisOfRecord,MMap[Long,Int]], v: (Feature.BasisOfRecord,MMap[Long,Int])) => {m+=((v._1, v._2))}
    val merge2 = (m1: MMap[Feature.BasisOfRecord,MMap[Long,Int]], m2:MMap[Feature.BasisOfRecord,MMap[Long,Int]]) => {
      // deep merge
      m2.foreach(e => {
        val bor = m1.getOrElseUpdate(e._1, MMap[Long,Int]())
        e._2.foreach( f => {
          bor(f._1) = bor.getOrElse(f._1, 0) + f._2
        })
      })
      m1
    }
    var tiles3 = tiles2.map(r => {
      // type, zxy -> bor : (pixel+year -> count)
      ((r._1._1,r._1._2),(r._1._3, r._2))
    }).aggregateByKey(MMap[Feature.BasisOfRecord,MMap[Long,Int]]().empty)(appendVal2, merge2)

    /**
      * For each zoom level working from max to min, we merge from the previous zoom (if required), then build
      * a buffer zone around each tile pulling in data from adjacent tiles, and then encode the data as vector tile
      * format, and finally produce HFiles.
      */
    (projectionConfig.minZoom to projectionConfig.maxZoom).reverse.foreach(z => {

      // downscale if needed, by merging the previous zoom tiles together (each a quad tiles become one tile)
      if (z != projectionConfig.maxZoom) {

        tiles3 = tiles3.map(t => {
          val zxy = MapUtils.fromZXY(t._1._2)
          val zoom = zxy._1
          val x = zxy._2
          val y = zxy._3

          val newTile = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
          t._2.foreach(keyVal => {
            val bor = newTile.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
            keyVal._2.foreach(feature => {
              val decoded = MapUtils.decodePixelYear(feature._1)
              val pixel = MapUtils.decodePixel(decoded._1)

              // Important(!)
              // We only emit pixels that fall on the current tile and exclude any that are in it's buffer.
              // If an e.g. eastzone buffer pixel of x=260 on a 256px tile  goes through the following code it would be
              // painted at 130px incorrectly.  We discard buffered pixels here as buffers are recreated if needed
              // after the tiles are downscaled.
              if (pixel._1 >= 0 && pixel._1 < projectionConfig.tileSize &&
                  pixel._2 >= 0 && pixel._2 < projectionConfig.tileSize) {
                val year = decoded._2

                // identify the quadrant it falls in, and adjust the pixel address accordingly
                val px = (pixel._1/2 + projectionConfig.tileSize/2 * (x%2)).asInstanceOf[Short]
                val py = (pixel._2/2 + projectionConfig.tileSize/2 * (y%2)).asInstanceOf[Short]
                val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
                bor(newKey) = bor.getOrElse(newKey, 0) + feature._2

              }
            })
          })
          // rewrite the X and Y which half each time you zoom out
          var newZXY = MapUtils.toZXY(z.asInstanceOf[Byte],x/2,y/2);
          ((t._1._1, newZXY), newTile)

        }).reduceByKey((a,b) => {
          // deep accumulation of the maps
          var result = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
          result ++= a
          b.foreach(e => {
            val bor = result.getOrElseUpdate(e._1, MMap[Long,Int]())
            e._2.foreach( f => {
              bor(f._1) = bor.getOrElse(f._1, 0) + f._2
            })
          })
          result
        })
      }

      val bufferSize = 8 // TODO - move into config parameter called "tileBufferSize"
      /**
        * Add a buffer to each tile by bringing in data from adjacent tiles.
        */
      // TODO: surround this with an if buffer > 0 ... else just copy tiles3
      val tiles4 = tiles3.flatMap(t => {
        val res = mutable.ArrayBuffer[((String, String), MMap[Feature.BasisOfRecord, MMap[Long,Int]])]()

        // pass through the original tile, and then emit the data that falls near the tile boundary, to collect into
        // the buffer zone of other tiles
        res += (t)

        // containers for pixels that will fall on tiles on ajacent cells (North, South etc naming)
        val tileN = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        val tileS = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        val tileE = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        val tileW = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        val tileNE = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        val tileNW = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        val tileSE = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        val tileSW = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()

        t._2.foreach(keyVal => {
          keyVal._2.foreach(feature => {
            val decoded = MapUtils.decodePixelYear(feature._1)
            val pixel = MapUtils.decodePixel(decoded._1)

            // North
            if (pixel._2 < bufferSize) {
              val bor = tileN.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = pixel._1.asInstanceOf[Short]
              val py = (pixel._2 + projectionConfig.tileSize).asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }

            // North West
            if (pixel._2 < bufferSize && pixel._1 < bufferSize) {
              val bor = tileNW.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = (pixel._1 + projectionConfig.tileSize).asInstanceOf[Short]
              val py = (pixel._2 + projectionConfig.tileSize).asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }

            // North East
            if (pixel._2 < bufferSize && pixel._1 >= projectionConfig.tileSize - bufferSize) {
              val bor = tileNE.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = (pixel._1 - projectionConfig.tileSize).asInstanceOf[Short]
              val py = (pixel._2 + projectionConfig.tileSize).asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }

            // West
            if (pixel._1 < bufferSize) {
              val bor = tileW.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = (pixel._1 + projectionConfig.tileSize).asInstanceOf[Short]
              val py = pixel._2.asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }

            // East
            if (pixel._1 >= projectionConfig.tileSize - bufferSize) {
              val bor = tileE.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = (pixel._1 - projectionConfig.tileSize).asInstanceOf[Short]
              val py = pixel._2.asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }

            // South
            if (pixel._2 >= projectionConfig.tileSize - bufferSize) {
              val bor = tileS.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = pixel._1.asInstanceOf[Short]
              val py = (pixel._2 - projectionConfig.tileSize).asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }

            // South West
            if (pixel._2 >= projectionConfig.tileSize - bufferSize && pixel._1 < bufferSize) {
              val bor = tileSW.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = (pixel._1 + projectionConfig.tileSize).asInstanceOf[Short]
              val py = (pixel._2 - projectionConfig.tileSize).asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }

            // South East
            if (pixel._2 >= projectionConfig.tileSize - bufferSize && pixel._1 >= projectionConfig.tileSize) {
              val bor = tileSE.getOrElseUpdate(keyVal._1, MMap[Long,Int]())
              val year = decoded._2
              val px = (pixel._1 - projectionConfig.tileSize).asInstanceOf[Short]
              val py = (pixel._2 - projectionConfig.tileSize).asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)
              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            }
          })
        })

        val mapKey = t._1._1;
        val zxy = MapUtils.fromZXY(t._1._2)
        val x = zxy._2
        val y = zxy._3

        // TODO: handle datelines and also include proper tests to determine if we are the poles
        // e.g. if you are on the bottom row there is no tile below to accumulate from, but the dateline does wrap.

        // North
        if (z>0 && !tileN.isEmpty && y>0) {
          res += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],x,y-1)),tileN))
        }
        // South
        if (z>0 && !tileS.isEmpty) {
          res += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],x,y+1)),tileS))
        }
        // East
        if (z>0 && !tileE.isEmpty) {
          res += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],x+1,y)),tileE))
        }
        // West
        if (z>0 && !tileW.isEmpty) {
          res += (((mapKey, MapUtils.toZXY(z.asInstanceOf[Byte],x-1,y)),tileW))
        }

        // TODO: NE,NW,SW,SE because I need to handle date line etc

        res
      }).reduceByKey((a,b) => {
        // deep accumulation of maps
        // TODO: move this into a util library as we do this a lot.
        var result = MMap[Feature.BasisOfRecord, MMap[Long,Int]]()
        result ++= a
        b.foreach(e => {
          val bor = result.getOrElseUpdate(e._1, MMap[Long,Int]())
          e._2.foreach( f => {
            bor(f._1) = bor.getOrElse(f._1, 0) + f._2
          })
        })
        result
      })

      /**
        * Generate the vector tile and write it as an HFile.
        */
      tiles4.mapValues(tile => {
        // set up the encoder with no buffer and false to indicate that the features are not 0..255 space, but
        // already in the the space of the tileSize
        val bufferSize = 8 // TODO!!! read from that config tileBufferSize we created above
        val encoder = new VectorTileEncoder(projectionConfig.tileSize, bufferSize, false)

        tile.keySet.foreach(bor => {
          val pixelYears = tile.get(bor)

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
            val point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel._1.toDouble, pixel._2.toDouble));
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
