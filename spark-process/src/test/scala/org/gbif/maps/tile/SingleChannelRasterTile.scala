package org.gbif.maps.tile

import scala.collection.mutable.{Map => MMap}

/**
  * Utility implementation for tests only, which stores a count by pixel (i.e. a basic single channel raster)
  */
private[tile]class SingleChannelRasterTile(zxy: ZXY) extends Tile[String, Int](zxy, 512, 64) {
  override def newTileInstance(zxy: ZXY): Tile[String, EncodedPixel] = new SingleChannelRasterTile(zxy)
  override def newFeatureDataInstance(): FeatureData[EncodedPixel] = new PixelCount
}

// A count at a pixel
private[tile]class PixelCount extends FeatureData[Int] {
  private[tile] val data = MMap[Pixel, Int]()

  override def iterator() = data.iterator
  override def featureCount() = data.size
  override def collect(pixel: Pixel, value: Int)  = {
    data(pixel) = data.getOrElse(pixel, 0) + value
  }
}
