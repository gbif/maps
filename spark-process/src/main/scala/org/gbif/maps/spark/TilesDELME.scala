package org.gbif.maps.spark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import no.ecc.vectortile.VectorTileEncoder
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.gbif.maps.common.projection.Mercator
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}

object TilesDELME extends Serializable {

  // Utility classes for data structures
  case class BoRYearRecord(mapType: Int, mapKey: Any, lat: Double, lng: Double, basisOfRecord: Feature.BasisOfRecord, year: Int, count: Int)
  case class TileKey(mapType: Int, mapKey: Any, z: Int, x: Long, y: Long) extends Ordered[TileKey] {
    // Requires lexicographic ordering (since used in HBase keys)
    def compare(that: TileKey): Int = {
      this.toString().compare(that.toString())
    }
    override def toString() : String = {mapType + ":" + mapKey + ":" + z + ":" + x + ":" + ":" + y }
  }

  private val TILE_SIZE = 4096
  private val MERCATOR = new Mercator(TILE_SIZE)
  private val GEOMETRY_FACTORY = new GeometryFactory()

  /**
    * Returns a new RDD holding pixel data projected to mercator
    *
    * @param rdd To build into a tiles
    * @param maxZoom The zoom level at which we are tiling
    * @return A KVP RDD where the value contains data per pixel
    */
  def toMercatorTilesOrig(rdd: RDD[BoRYearRecord], zoom: Int): RDD[(TileKey, mutable.Map[(Int, Int), mutable.Map[Int, Int]])] = {
    rdd.map(r => {
      val x = MERCATOR.longitudeToTileX(r.lng, zoom.asInstanceOf[Byte])
      val y = MERCATOR.latitudeToTileY(r.lat, zoom.asInstanceOf[Byte])
      val px = MERCATOR.longitudeToTileLocalPixelX(r.lng, zoom.asInstanceOf[Byte])
      val py = MERCATOR.latitudeToTileLocalPixelY(r.lat, zoom.asInstanceOf[Byte])
      (new TileKey(r.mapType, r.mapKey, zoom, x, y),(px, py, r.basisOfRecord, r.year, r.count))
    }).aggregateByKey(
      // now aggregate by the key to combine data in a tile
      mutable.Map[(Int, Int), mutable.Map[Int, Int]]())((agg, v) => {
      // px:py to year:count
      // TODO: BoR handling(!)
      agg.put((v._1, v._2), mutable.Map(v._4-> v._5))
      agg
    }, (agg1, agg2) => {
      // merge and convert into a mutable object
      mutable.Map() ++ Maps.merge(agg1, agg2)
    })
  }

  /**
    * TEST!
    */
  def toMercatorTiles(rdd: RDD[BoRYearRecord], zoom: Int) : RDD[(TileKey, ((Int,Int),(Feature.BasisOfRecord,Int,Int)))] = {
    rdd.map(r => {
      val x = MERCATOR.longitudeToTileX(r.lng, zoom.asInstanceOf[Byte])
      val y = MERCATOR.latitudeToTileY(r.lat, zoom.asInstanceOf[Byte])
      val px = MERCATOR.longitudeToTileLocalPixelX(r.lng, zoom.asInstanceOf[Byte])
      val py = MERCATOR.latitudeToTileLocalPixelY(r.lat, zoom.asInstanceOf[Byte])
      ((r.mapType, r.mapKey, x, y, px, py, r.basisOfRecord, r.year),r.count)

    }).reduceByKey(_ + _).map( r => {
      val k = r._1
      val v = r._2
      // TileKey : Pixel : Year+BoR+Count
      (new TileKey(k._1, k._2, zoom, k._3, k._4), ((k._5, k._6),(k._7, k._8, v)))
    })
  }

  /**
    * Downscales the given RDD of tiles by one zoom.  E.g. Given an RDD containing tiles for zoom level 4, will return
    * a new RDD containing the tiles at zoom level 3.
    *
    * TODO: At the moment this only works for year to count data per pixel.  It does not work for BasisOfRecord
    *
    * @param rdd To downscale
    * @return A new, smaller RDD where the data is downscaled by 1 zoom
    */
  def downscale(rdd: RDD[(TileKey, mutable.Map[(Int, Int), mutable.Map[Int, Int]])])
    : RDD[(TileKey, mutable.Map[(Int, Int), mutable.Map[Int, Int]])] = {
    rdd.map(t => {
      if (t._1.z == 0) throw new IllegalStateException("Cannot downscale lower than zoom 0")

      val pixels = mutable.Map[(Int, Int), mutable.Map[Int, Int]]();
      t._2.foreach(p => {
        // shift the pixel from the previous zoom to this zoom
        val px = p._1._1/2 + TILE_SIZE/2 * (t._1.x.asInstanceOf[Int]%2)
        val py = p._1._2/2 + TILE_SIZE/2 * (t._1.y.asInstanceOf[Int]%2)

        // add or merge the pixel data
        if (!pixels.contains((px,py))) pixels.put((px,py), p._2)
        else pixels.get((px,py)) ++ pixels // mutable maps
      })

      val key = new TileKey(t._1.mapType, t._1.mapKey, t._1.z-1, t._1.x/2, t._1.y/2)
      (key, pixels)
    }).reduceByKey((a,b) => mutable.Map() ++ Maps.merge(a, b))
  }

  /**
    * Converts the given RDD of tiles containing pixel data into an RDD of Mapbox Vector Tiles.
    * @param rdd A key value RDD to convert
    * @return A new RDD keyed the same, but with the values all encoded as vector tiles
    */
  def toVectorTileOrig(rdd: RDD[(TileKey, mutable.Map[(Int, Int), mutable.Map[Int, Int]])]) : RDD[(TileKey, Array[Byte])] = {
    rdd.map(r => {
      val encoder = new VectorTileEncoder(4096, 0, false); // for each entry (a pixel, px) we have a year:count Map

      r._2.foreach(px => {
        val point = GEOMETRY_FACTORY.createPoint(new Coordinate(px._1._1.toDouble, px._1._2.toDouble));
        val meta = new java.util.HashMap[String, Any]() // TODO: add metadata(!)
        encoder.addFeature("points", meta, point);
      })

      (r._1, encoder.encode())
    })
  }

  /**
    * TEST!
    */
  def toVectorTile(rdd: RDD[(TileKey, ((Int,Int),(Feature.BasisOfRecord,Int,Int)))]) : RDD[(TileKey, Array[Byte])] = {
    rdd.groupByKey().map(r => {
      val encoder = new VectorTileEncoder(4096, 0, false); // for each entry (a pixel, px) we have a year:count Map

      r._2.toMap.keys.foreach(px => {

      //r._2.foreach(px => {
        val point = GEOMETRY_FACTORY.createPoint(new Coordinate(px._1.toDouble, px._2.toDouble));
        val meta = new java.util.HashMap[String, Any]() // TODO: add metadata(!)
        encoder.addFeature("points", meta, point);
      })

      (r._1, encoder.encode())
    })
  }
}

