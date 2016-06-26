package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.sql.DataFrame
import org.gbif.maps.common.projection.Tiles
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable.{Map => MMap}
import scala.collection.{Set, mutable}

/**
  * A builder of HFiles for a tile pyramid.
  */
object BackfillTiles {
  private val GEOMETRY_FACTORY = new GeometryFactory()

  /**
    * A partitioner that puts pixels in the same category together
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
    }).reduceByKey(_+_, 200).map(r => {
      // type, zxy, bor : pixel,year,count
      ((r._1._1, r._1._2, r._1._4),(r._1._3,r._1._5,r._2))
    }).partitionBy(new TileGroupPartitioner(200))

    // collect a map of pixelYear -> count
    val appendVal = (m: MMap[Long,Int], v: (Int,Short,Int)) => {
      val py = MapUtils.encodePixelYear(v._1,v._2)
      m+=((py, v._3))
    }
    val merge = (m1: MMap[Long,Int], m2: MMap[Long,Int]) => {
      m1 ++= m2
    }
    val tiles2 = tiles.aggregateByKey(MMap[Long,Int]().empty)(appendVal, merge)

    // collect a map of category -> pixelYear -> count
    val appendVal2 = (m: MMap[Feature.BasisOfRecord,MMap[Long,Int]], v: (Feature.BasisOfRecord,MMap[Long,Int])) => {m+=((v._1, v._2))}
    val merge2 = (m1: MMap[Feature.BasisOfRecord,MMap[Long,Int]], m2:MMap[Feature.BasisOfRecord,MMap[Long,Int]]) => {
      // deep merge
      m2.foreach(e => {
        val bor = m1.getOrElseUpdate(e._1, MMap[Long,Int]())
        e._2.foreach( f => {
          bor(f._1) = bor.getOrElse(f._1, 0) + f._2
        })
      })
      m1  // TODO: Can we reuse or not?

    }
    var tiles3 = tiles2.map(r => {
      ((r._1._1,r._1._2),(r._1._3, r._2))
    }).aggregateByKey(MMap[Feature.BasisOfRecord,MMap[Long,Int]]().empty)(appendVal2, merge2)

    (projectionConfig.minZoom to projectionConfig.maxZoom).reverse.foreach(z => {

      // downscale if needed
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
              val year = decoded._2
              val px = (pixel._1/2 + projectionConfig.tileSize/2 * (x%2)).asInstanceOf[Short]
              val py = (pixel._2/2 + projectionConfig.tileSize/2 * (y%2)).asInstanceOf[Short]
              val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(px,py), year)

              bor(newKey) = bor.getOrElse(newKey, 0) + feature._2
            })
          })
          // rewrite the X and Y which half each time you zoom out
          ((t._1._1, z + ":" + x/2 + ":" + y/2), newTile)

        }).reduceByKey((a,b) => {
          // deep copy
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

      // write the tile
      tiles3.mapValues(tile => {
        // set up the encoder with no buffer and false to indicate that the features are not 0..255 space, but
        // already in the the space of the TILE_SIZE
        val encoder = new VectorTileEncoder(projectionConfig.tileSize, 0, false)

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
          Bytes.toBytes("merc_tiles"), // column family
          Bytes.toBytes(cell), // cell
          cellData)
        (k, row)
      }).saveAsNewAPIHadoopFile(config.targetDirectory + "/" + projectionConfig.srs.replaceAll(":", "_") + "/tiles/z" + z, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], Configurations.hfileOutputConfiguration(config, config.tilePyramid.tableName))
    })
  }
}
