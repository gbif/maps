package org.gbif.maps.spark

import org.gbif.maps.io.PointFeature.PointFeatures
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import collection.mutable.{Map => MMap}
import collection.immutable.{Map => IMap}

class DensityTile extends Serializable {
  private val data = MMap[(Int,Int), CategoryYearCount[Feature.BasisOfRecord]]()
  private val TILE_SIZE = 4096

  def collect(pixel: (Int,Int), category: Feature.BasisOfRecord, year: Int, count: Int) : DensityTile = {
    val features = data.getOrElseUpdate(pixel, new CategoryYearCount[Feature.BasisOfRecord]())
    features.collect(category, year, count)
    this
  }

  def merge(t: DensityTile) : DensityTile = {
    t.build().foreach(e1 => {
      e1._2.foreach(e2 => {
        e2._2.foreach(e3 => {
          collect(e1._1, e2._1, e3._1, e3._2)
        })
      })
    })
    this
  }

  /**
    * Downscales the tile by 1 zoom
    * @param z The zoom from which it is being downscaled
    * @param x The previous tile address X
    * @param y The previous tile address Y
    * @return A new tile suitable for use at 1 lower zoom level (requires to be merged with 3 others)
    */
  def downscale(z: Int, x: Int, y:Int) : DensityTile = {
    if (z <= 0) throw new IllegalStateException("Cannot downscale lower than zoom 0")
    val t = new DensityTile()
    data.foreach(e1 => {
      e1._2.build().foreach(e2 => {
        e2._2.foreach(e3 => {
          val px = e1._1._1/2 + TILE_SIZE/2 * (x%2)
          val py = e1._1._2/2 + TILE_SIZE/2 * (y%2)
          t.collect((px,py), e2._1, e3._1, e3._2)
        })
      })
    })
    t
  }

  /**
    * @return An immutable copy of the data
    */
  def build() : IMap[(Int,Int), IMap[Feature.BasisOfRecord,IMap[Int,Int]]] = {
    data.map(kv => (kv._1,kv._2.build())).toMap
  }
}
