package org.gbif.maps.tile

import com.google.common.annotations.VisibleForTesting
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable.{Map => MMap}

/**
  * A tile which stores a count per year for each pixel separately for each basisOfRecord.  What the count represents
  * is arbitrary, but the most common use would be a record count to produce a classic GBIF density map.
  * <p/>
  * This uses an optimised storage structure which encodes the year and pixel into 64 bits.  The optimisation of this
  * is encapsulated in the [[EncodedPixelYearCount]]
  *
  * @param zxy The tile address
  * @param tileSize The tile size in pixels
  * @param bufferSize The buffer size in pixels
  */
class OccurrenceDensityTile(zxy: ZXY, tileSize: Int, bufferSize: Int)
  extends Tile[Feature.BasisOfRecord, YearCount](zxy, tileSize, bufferSize) with Serializable {

  /**
    * Returns a new [[OccurrenceDensityTile]] instance for the given address using the same tileSize and bufferSize as
    * ourselves.
    * @param zxy The tile address for the new tile
    * @return The new tile instance
    */
  override def newTileInstance(zxy: ZXY) = new OccurrenceDensityTile(zxy, tileSize, bufferSize)

  /**
    * Creates a new empty tile of the same address and dimensions as this tile
    * @return The new instance
    */
  def newTileInstance() = new OccurrenceDensityTile(zxy, tileSize, bufferSize)

  /**
    * Returns a new [[EncodedPixelYearCount]] instance.
    * @return A new instance
    */
  override def newFeatureDataInstance() = new EncodedPixelYearCount()

  /**
    * Collects the provided features into this tile, merging the counts if required.
    *
    * @param basisOfRecord Identifying the layer within which we should collect into
    * @param pixelData The data to collect
    * @return Ourselves (this)
    */
  def collect(basisOfRecord: Feature.BasisOfRecord, pixelData: MMap[EncodedPixelYear,Int]) : OccurrenceDensityTile = {
    // locate the layer and collect the data
    val layer = getData().getOrElseUpdate(basisOfRecord, newFeatureDataInstance()).asInstanceOf[EncodedPixelYearCount]
    layer.collect(pixelData)
    this
  }
}

/**
  * "Static methods" on DensityTile
  */
object OccurrenceDensityTile {

  /**
    * Merges the provided tiles into a new instance without mutating the sources
    *
    * @param sources The source tiles to merge
    * @return A new instance
    */
  def merge(sources: OccurrenceDensityTile*) : OccurrenceDensityTile = {
    // a new tile into which we will accumulate data
    val target = sources(0).newTileInstance()

    // for each source tile being merged
    for (sourceTile <- sources) {

      // for each layer of basisOfRecord in the source tile
      for ((basisOfRecord, features) <- sourceTile.getData()) {

        // create the layer in the target tile if needed
        val targetLayer = target.getData().getOrElseUpdate(basisOfRecord, target.newFeatureDataInstance())

        // accumulate the data in the target layer
        for ((pixel, yearCount) <- features.iterator()) {
          targetLayer.collect(pixel, yearCount)
        }
      }
    }
    target
  }
}

/**
  * A structure that stores counts by year for pixels in a performance optimised manner.
  */
class EncodedPixelYearCount extends FeatureData[YearCount] with Serializable {
  @VisibleForTesting
  private[tile] val data = MMap[EncodedPixelYear, Int]()

  /**
    * @return An iterator presenting the features per pixel
    */
  override def iterator() : Iterator[(Pixel, YearCount)] = {
    data.toIterator.map(e => {
      val (pixelYear, count) = e
      val (encodedPixel, year) = decodePixelYear(pixelYear)
      val pixel = decodePixel(encodedPixel)
      (pixel, new YearCount(year, count))
    })
  }

  /**
    * Collects the feature provided into the internal data structure.
    *
    * @param pixel Pixel
    * @param feature Feature to encode for the pixel
    */
  override def collect(pixel: Pixel, feature: YearCount)  = {
    val encodedPixel = encodePixel(pixel)
    val encodedPixelYear = encodePixelYear(encodedPixel, feature.year)
    data(encodedPixelYear) = data.getOrElse(encodedPixelYear, 0) + feature.count
  }

  /**
    * Collects the pre-encoded data accumulating the counts if required.
    *
    * @param pixelData A pre-encoded map of data to merge in to the internal data
    */
  def collect(pixelData: MMap[EncodedPixelYear,Int]): Unit = {
    for ((encodedPixelYear, count) <- pixelData) {
      data(encodedPixelYear) = data.getOrElse(encodedPixelYear, 0) + count
    }
  }

  /**
    * Returns total the number of features contained.
    *
    * @return The number of features (i.e. 3 pixels, each with 5 years of data would provide 15 features)
    */
  override def featureCount(): EncodedPixel = data.size
}
