package org.gbif.maps.tile

import scala.collection.mutable.{Map => MMap}
import org.gbif.maps.tile.TileUtils.Region

/**
  * Defines the interface that must be implemented by tiles to present their features in a typed manner.
  *
  * @tparam FT The feature type stored per pixel
  */
trait FeatureData[FT] {

  /**
    * Returns an iterator of presenting the features.
    *
    * @return Iterator of tuple pairs containing the pixel and a feature at that pixel
    */
  def iterator(): Iterator[(Pixel, FT)]

  /**
    * Collects a features at the pixel.
    *
    * @param pixel That is to have the feature attached
    * @param feature The feature to associate with the pixel
    */
  def collect(pixel: Pixel, feature: FT)

  /**
    * Returns total the number of features contained.
    * @return The number of features (i.e. 3 pixels, each with 5 attributes would be 15 features)
    */
  def featureCount() : Int
}

/**
  * The base class of all tiles.  A tile contains a data structure whereby there are separate layers and each layer
  * has a collections of features.
  * <p/>
  * This provides generic functionality to expose the regions of the tile which overlap with adjacent tiles such that
  * they can used as buffers when stitched on to the adjacent tile.
  *
  * @param zxy The tile address
  * @param tileSize The tile size in pixels
  * @param bufferSize The buffer size in pixels
  * @tparam L The layer type
  * @tparam F The feature type
  */
abstract class Tile[L, F](zxy: ZXY, tileSize: Int, bufferSize: Int) extends Serializable {

  // the data structure split into layers
  private val data = MMap[L, FeatureData[F]]()

  // exposes our data
  def getData() = data

  // exposes the tile address
  def getZXY() = zxy

  /**
    * Collects a feature at a pixel.
    * @param layer To collect into
    * @param pixel The pixel for which to append the feature to
    * @param feature The feature
    * @return Ourself
    */
  def collect(layer: L, pixel: Pixel, feature: F) : Tile[L, F] = {
    val layerData = getData().getOrElseUpdate(layer, newFeatureDataInstance())
    layerData.collect(pixel, feature)
    this
  }

  /**
    * Returns a set of tiles, each representing a region of the current tile with the pixels readdressed so they can
    * be used directly in the adjacent tile as a buffer.  Optionally this can downscale at the same time to prepare
    * a tile at e.g zoom 15 into a set of tiles suitable for stitching together at zoom 14.
    *
    * @param downscale true if the pixels should be readdressed for use at 1 zoom lower than the current tile
    * @param wrapDateline true if the pixel addressing should wrap across the international dateline
    * @return A set of tiles representing all regions of this tile suitable for stitching together with adjacent tiles
    */
  def flatMapToBuffers(downscale: Boolean, wrapDateline: Boolean) : Iterable[Tile[L,F]] = {
    val result = MMap[Region.Value, Tile[L,F]]()

    // determine the possible regions of interest for this tile that can be used as buffers on adjacent tiles
    val regions = TileUtils.bufferRegions(zxy, downscale, wrapDateline)

    // for each layer
    for ((layer, features) <- data) {

      // for each feature within the layer
      for((pixel, feature) <- features.iterator()) {

        // To avoid double counting exclude any pixels in our buffer zone
        if (pixel.x >= 0 && pixel.x < tileSize && pixel.y >= 0 && pixel.y < tileSize) {

          // adjust the tile and pixel addresses for the new zoom if necessary
          val downscaledZXY = TileUtils.downscaleAddress(zxy, downscale)
          val downscaledPixel = TileUtils.downscalePixel(zxy, tileSize, pixel, downscale)

          // for each region, adjust the pixel for the target tile and collect the results
          for (region <- regions) {
            val bufferAdjustedPixel = TileUtils.adjustPixelForBuffer(downscaledPixel, region, tileSize, bufferSize)

            bufferAdjustedPixel match {
              case Some(pixel) => appendFeature(result, region, downscaledZXY, layer, pixel, feature)
              case None => None // pixel ignored for this region
            }
          }
        }
      }
    }
    result.values
  }

  /**
    * Appends a feature to the context.  When the context does not contain the target tile it is created.
    *
    * @param context To collect the feature into
    * @param region The key within the context within which we are working
    * @param layer The layer within which we are working
    * @param pixel To collect
    * @param feature To associate with the pixel
    */
  private[tile] def appendFeature(context: MMap[Region.Value, Tile[L,F]], region: Region.Value, zxy: ZXY, layer: L, pixel: Pixel,
    feature: F) = {
    var targetTileZXY = TileUtils.adjacentTileZXY(zxy, region)
    val targetTile = context.getOrElseUpdate(region, newTileInstance(targetTileZXY))
    val layerData = targetTile.getData().getOrElseUpdate(layer, newFeatureDataInstance())
    layerData.collect(pixel, feature)
  }

  /**
    * Constructs a new instance of the tile of the correct type.
    * @param zxy The address for the new tile
    * @return A new tile of the required type
    */
  def newTileInstance(zxy: ZXY) : Tile[L,F]

  /**
    * Constructs a new instance of the feature data for the tile.
    *
    * @return The new typed instance of the feature data, which will be associated with a layer
    */
  def newFeatureDataInstance() : FeatureData[F]
}
