package org.gbif.maps.spark



import com.google.common.annotations.VisibleForTesting
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable.{Map => MMap, Set => MSet}

case class YearCount(year: Year, count: Int)

// TODO: rename when we remove the package version
case class ZXY2(z: Int, x: Long, y: Long)


abstract class Tile3[L, F] {
  val data = MMap[L, FeatureData[F]]()
  def getData() : MMap[L, FeatureData[F]] = {data}
}


// utils
object Tiles2 {
  /**
    * The regions of a tile.
    */
  object Region extends Enumeration {
    val N, S, E, W, NE, NW, SE, SW, CENTER = Value
  }


  /*
  def flatMapToBuffers[L,F](zxy: ZXY2, tile: Tile3[L,F], tileSize: Int, bufferSize: Int, downscale: Boolean, wrapDateline: Boolean) : Set[Tile[L,F]] = {
    val result = MMap[Region.Value, Tile3[L,F]]()

    // determine the possible regions of interest for this tile that can be used as buffers on adjacent tiles
    val regions = bufferRegions(zxy, downscale, wrapDateline)

    // for each layer
    for ((layer, features) <- tile.getData()) {

      // for each feature within the layer
      for((pixel, feature) <- features.iterator()) {

        // To avoid double counting exclude any pixels in our buffer zone
        if (pixel.x >= 0 && pixel.x < tileSize && pixel.y >= 0 && pixel.y < tileSize) {

          // adjust the pixel address for a new zoom if necessary
          val downscaledPixel = downscalePixel(zxy, tileSize, pixel, downscale)

          // for each region, adjust the pixel for the target tile and collect the results
          for (region <- regions) {
            val bufferAdjustedPixel = adjustPixelForBuffer(downscaledPixel, region, tileSize, bufferSize)

            bufferAdjustedPixel match {
              case None => None // pixel ignored for this region
              case Some(pixel) => {
                val targetZXY = adjacentTile(zxy, region)

                //appendPixel(result, region, layer, pixel, feature)
              }

            }
          }
        }
      }
    }
    //result.values.toSet
    None
  }
  */

  def adjacentTile(source:ZXY2, direction: Region.Value): ZXY2 = {
    return source // TODO!!!
  }

  /**
    * Adjusts the pixel location for use in a tile 1 zoom lower if required.
    * @param pixel To downscale
    * @param downscale If true adjusts the pixel, otherwise this method returns the input pixel
    * @return The downscaled pixel or input pixel
    */
  @VisibleForTesting
  def downscalePixel(zxy:ZXY2, tileSize: Int, pixel: Pixel, downscale: Boolean) : Pixel = {
    if (downscale) {
      // identify the quadrant the current tile falls in and adjust the pixel address accordingly ready to be used
      // in the tile it will fall in at one zoom lower
      val px = (pixel.x/2 + tileSize/2 * (zxy.x%2)).asInstanceOf[Short]
      val py = (pixel.y/2 + tileSize/2 * (zxy.y%2)).asInstanceOf[Short]
      return Pixel(px,py)
    }
    return pixel
  }

  /**
    * Returns the regions of the tile that will overlap with buffer zone of an adjacent tile.  This is useful when
    * stitching together tiles to produce tiles with buffers.
    *
    * @param z The current zoom level
    * @param x The tile x address
    * @param y The tile y address
    * @param tileSize The tile size in pixels
    * @param downscale If this is downscaling at the same time as buffering
    * @param wrapDateline If the eastern and western hemispheres should wrap and buffer
    * @return The areas of interest
    */
  @VisibleForTesting
  def bufferRegions(zxy: ZXY2, downscale: Boolean , wrapDateline: Boolean) : Set[Region.Value] = {
    // initialise the result, knowing that our own canvas is always of interest
    val result = MSet[Region.Value](Region.CENTER)

    if (downscale) {
      // if we are downscaling then only the zones which will be
      (zxy.x % 2, zxy.y % 2) match {
        case (0, 0) => result ++= Set(Region.N, Region.W, Region.NW)
        case (1, 0) => result ++= Set(Region.N, Region.E, Region.NE)
        case (0, 1) => result ++= Set(Region.S, Region.W, Region.SW)
        case (1, 1) => result ++= Set(Region.S, Region.E, Region.SE)
      }
    } else {
      // all regions may be of interest
      result ++= Set(Region.N, Region.NE, Region.E, Region.SE,
        Region.S, Region.SW, Region.W, Region.NW)
    }

    val targetZ = if (downscale) zxy.z - 1 else zxy.z
    val maxTileAddress = if (targetZ == 0) 0 else (2 << (targetZ - 1)) - 1
    val targetX = if (downscale) zxy.x >>> 1 else zxy.x
    val targetY = if (downscale) zxy.y >>> 1 else zxy.y

    // trim to the world handling dateline wrapping if desirable
    if (targetY == 0) {
      result -=(Region.N, Region.NE, Region.NW) // nothing north of us
    }
    if (targetY == maxTileAddress) {
      result -=(Region.S, Region.SE, Region.SW) // nothing south of us
    }
    if (targetX == 0 && !wrapDateline) {
      result -=(Region.NW, Region.W, Region.SW) // nothing west of us
    }
    if (targetX == maxTileAddress && !wrapDateline) {
      result -=(Region.NE, Region.E, Region.SE) // nothing east of us
    }

    // this is a special case, not covered by the rules above
    if (zxy.z == 0 && wrapDateline) {
      result ++= Set(Region.E, Region.W)
    }
    return result.toSet
  }

  /**
    * Readdresses the given pixel to the reference system of an adjacent tile if the pixel will fall in it's buffer
    * zone.
    * <p/>
    * The adjacent tile is determined by the region supplied such that North means the target tile is directly above
    * this tile, South-East is below and to the right etc.
    * <p/>
    * As an example given a pixel of 10,10 and a region of North, this will readdress to 10,266 when dealing with 256
    * sized tiles.
    *
    * @param pixel To readdress to the a new reference
    * @param region Defining the adjacent tile to which this pixel is destined
    * @return The pixel adjusted to the coordinate referencing system of the target tile
    */
  @VisibleForTesting
  def adjustPixelForBuffer(pixel: Pixel, region: Region.Value, tileSize: Int, bufferSize: Int) : Option[Pixel] = {
    // convience container describing the adjustment to make
    case class Adjustment (minX: Int, minY: Int, maxX: Int, maxY: Int, offsetX: Int, offsetY: Int)

    // Depending on the region we apply different adjustments to the pixel location.  E.g. a pixel destined to the
    // tile directly above, needs the pixel Y address offset to the reference of that tile, which is a 1 x tileSize
    // adjustment
    val adjustment = region match {
      case Region.CENTER => new Adjustment(0, 0, tileSize, tileSize, 0, 0) // no adjustment
      case Region.N => new Adjustment(0, 0, tileSize, bufferSize, 0, tileSize)
      case Region.NE => new Adjustment(tileSize-bufferSize, 0, tileSize, bufferSize, -tileSize, tileSize)
      case Region.E => new Adjustment(tileSize-bufferSize, 0, tileSize, tileSize, -tileSize, 0)
      case Region.SE => new Adjustment(tileSize-bufferSize, tileSize-bufferSize, tileSize, tileSize, -tileSize, -tileSize)
      case Region.S => new Adjustment(0, tileSize-bufferSize, tileSize, tileSize, 0, -tileSize)
      case Region.SW => new Adjustment(0, tileSize-bufferSize, bufferSize, tileSize, tileSize, -tileSize)
      case Region.W => new Adjustment(0, 0, bufferSize, tileSize, tileSize, 0)
      case Region.NW => new Adjustment(0, 0, bufferSize, bufferSize, tileSize, tileSize)
    }

    // only if the pixel falls within our area of interest, adjust the coordinates and add the contents to the target
    if (pixel.x >= adjustment.minX && pixel.x < adjustment.maxX &&
        pixel.y >= adjustment.minY && pixel.y < adjustment.maxY) {

      val newPixel = new Pixel((pixel.x + adjustment.offsetX).asInstanceOf[Short],
        (pixel.y + adjustment.offsetY).asInstanceOf[Short]);
      return Option(newPixel)
    }
    return None
  }
}


/**
  * A base class for dealing with tiles and providing utility
  *
  * @param z
  * @param x
  * @param y
  * @param size
  * @param buffer
  * @tparam L layer type
  * @tparam F feature type
  *
  */
abstract class Tile[L, F](z: Int, x: Long, y: Long, tileSize: Int, bufferSize: Int) {

  val data = MMap[L, FeatureData[F]]()

  def getData() : MMap[L, FeatureData[F]] = {data}

  /**
    * The regions of a tile.
    */
  object Region extends Enumeration {
    val N, S, E, W, NE, NW, SE, SW, CENTER = Value
  }


  def flatMapToBuffers(downscale: Boolean, wrapDateline: Boolean) : Set[Tile[L,F]] = {
    val result = MMap[Region.Value, Tile[L,F]]()

    // determine the possible regions of interest for this tile that can be used as buffers on adjacent tiles
    val regions = bufferRegions(downscale, wrapDateline)

    // for each layer
    for ((layer, features) <- data) {

      // for each feature within the layer
      for((pixel, feature) <- features.iterator()) {

        // To avoid double counting exclude any pixels in our buffer zone
        if (pixel.x >= 0 && pixel.x < tileSize && pixel.y >= 0 && pixel.y < tileSize) {

          // adjust the pixel address for a new zoom if necessary
          val downscaledPixel = downscalePixel(pixel, downscale)

          // for each region, adjust the pixel for the target tile and collect the results
          for (region <- regions) {
            val bufferAdjustedPixel = adjustPixelForBuffer(downscaledPixel, region)

            bufferAdjustedPixel match {
              case Some(pixel) => appendPixel(result, region, layer, pixel, feature)
              case None => None // pixel ignored for this region
            }
          }
        }
      }
    }
    result.values.toSet
  }

  /**
    * Readdresses the given pixel to the reference system of an adjacent tile if the pixel will fall in it's buffer
    * zone.
    * <p/>
    * The adjacent tile is determined by the region supplied such that North means the target tile is directly above
    * this tile, South-East is below and to the right etc.
    * <p/>
    * As an example given a pixel of 10,10 and a region of North, this will readdress to 10,266 when dealing with 256
    * sized tiles.
    *
    * @param pixel To readdress to the a new reference
    * @param region Defining the adjacent tile to which this pixel is destined
    * @return The pixel adjusted to the coordinate referencing system of the target tile
    */
  @VisibleForTesting
  def adjustPixelForBuffer(pixel: Pixel, region: Region.Value) : Option[Pixel] = {
    // convience container describing the filter to test and the adjustment to make
    case class Adjustment (minX: Int, minY: Int, maxX: Int, maxY: Int, offsetX: Int, offsetY: Int)

    // Depending on the region we apply different adjustments to the pixel location.  E.g. a pixel destined to the
    // tile directly above, needs the pixel Y address offset to the reference of that tile, which is a 1 x tileSize
    // adjustment
    val adjustment = region match {
      case Region.CENTER => new Adjustment(0, 0, tileSize, tileSize, 0, 0) // no adjustment
      case Region.N => new Adjustment(0, 0, tileSize, bufferSize, 0, tileSize)
      case Region.NE => new Adjustment(tileSize-bufferSize, 0, tileSize, bufferSize, -tileSize, tileSize)
      case Region.E => new Adjustment(tileSize-bufferSize, 0, tileSize, tileSize, -tileSize, 0)
      case Region.SE => new Adjustment(tileSize-bufferSize, tileSize-bufferSize, tileSize, tileSize, -tileSize, -tileSize)
      case Region.S => new Adjustment(0, tileSize-bufferSize, tileSize, tileSize, 0, -tileSize)
      case Region.SW => new Adjustment(0, tileSize-bufferSize, bufferSize, tileSize, tileSize, -tileSize)
      case Region.W => new Adjustment(0, 0, bufferSize, tileSize, tileSize, 0)
      case Region.NW => new Adjustment(0, 0, bufferSize, bufferSize, tileSize, tileSize)
    }

    // only if the pixel falls within our area of interest, adjust the coordinates and add the contents to the target
    if (pixel.x >= adjustment.minX && pixel.x < adjustment.maxX &&
        pixel.y >= adjustment.minY && pixel.y < adjustment.maxY) {

      val newPixel = new Pixel((pixel.x + adjustment.offsetX).asInstanceOf[Short],
        (pixel.y + adjustment.offsetY).asInstanceOf[Short]);
      return Option(newPixel)
    }
    return None
  }

  def appendPixel(context: MMap[Region.Value, Tile[L,F]], region: Region.Value, layer: L, pixel: Pixel, feature: F) = {
    val targetZXY = adjacentTile(new ZXY2(z,x,y), region)
    val targetTile = context.getOrElseUpdate(region, newTileInstance(targetZXY.z, targetZXY.x, targetZXY.y))
    val layerData = targetTile.getData().getOrElseUpdate(layer, newFeatureDataInstance())
    layerData.collect(pixel, feature)
  }

  def newTileInstance(z: Int, x: Long, y: Long) : Tile[L,F]
  def newFeatureDataInstance() : FeatureData[F]

  def adjacentTile(source:ZXY2, direction: Region.Value): ZXY2 = {
    return source // TODO!!!
  }


  /**
    * Adjusts the pixel location for use in a tile 1 zoom lower if required.
    * @param pixel To downscale
    * @param downscale If true adjusts the pixel, otherwise this method returns the input pixel
    * @return The downscaled pixel or input pixel
    */
  @VisibleForTesting
  def downscalePixel(pixel: Pixel, downscale: Boolean) : Pixel = {
    if (downscale) {
      // identify the quadrant the current tile falls in and adjust the pixel address accordingly ready to be used
      // in the tile it will fall in at one zoom lower
      val px = (pixel.x/2 + tileSize/2 * (x%2)).asInstanceOf[Short]
      val py = (pixel.y/2 + tileSize/2 * (y%2)).asInstanceOf[Short]
      return Pixel(px,py)
    }
    return pixel
  }


  /**
    * Returns the regions of the tile that will overlap with buffer zone of an adjacent tile.  This is useful when
    * stitching together tiles to produce tiles with buffers.
    *
    * @param z The current zoom level
    * @param x The tile x address
    * @param y The tile y address
    * @param tileSize The tile size in pixels
    * @param downscale If this is downscaling at the same time as buffering
    * @param wrapDateline If the eastern and western hemispheres should wrap and buffer
    * @return The areas of interest
    */
  @VisibleForTesting
  def bufferRegions(downscale: Boolean , wrapDateline: Boolean) : Set[Region.Value] = {
    // initialise the result, knowing that our own canvas is always of interest
    val result = MSet[Region.Value](Region.CENTER)

    if (downscale) {
      // if we are downscaling then only certain zones are possible as others lie in the center of the target tile
      (x % 2, y % 2) match {
        case (0, 0) => result ++= Set(Region.N, Region.W, Region.NW)
        case (1, 0) => result ++= Set(Region.N, Region.E, Region.NE)
        case (0, 1) => result ++= Set(Region.S, Region.W, Region.SW)
        case (1, 1) => result ++= Set(Region.S, Region.E, Region.SE)
      }
    } else {
      // all regions may be of interest
      result ++= Set(Region.N, Region.NE, Region.E, Region.SE,
        Region.S, Region.SW, Region.W, Region.NW)
    }

    val targetZ = if (downscale) z - 1 else z
    val maxTileAddress = if (targetZ == 0) 0 else (2 << (targetZ - 1)) - 1
    val targetX = if (downscale) x >>> 1 else x
    val targetY = if (downscale) y >>> 1 else y

    // trim to the world handling dateline wrapping if desirable
    if (targetY == 0) {
      result -=(Region.N, Region.NE, Region.NW) // nothing north of us
    }
    if (targetY == maxTileAddress) {
      result -=(Region.S, Region.SE, Region.SW) // nothing south of us
    }
    if (targetX == 0 && !wrapDateline) {
      result -=(Region.NW, Region.W, Region.SW) // nothing west of us
    }
    if (targetX == maxTileAddress && !wrapDateline) {
      result -=(Region.NE, Region.E, Region.SE) // nothing east of us
    }

    // special case, not covered by the rules above
    if (targetZ == 0 && wrapDateline) {
      result ++= Set(Region.E, Region.W)
    }
    return result.toSet
  }
}



class OccurrenceDensityTile(z: Int, x: Long, y: Long, tileSize: Int, bufferSize: Int)
  extends Tile[Feature.BasisOfRecord, YearCount](z, x, y, tileSize, bufferSize) {

  override def newTileInstance(z: Int, x: Long, y: Long) = new OccurrenceDensityTile(z, x, y, tileSize, bufferSize)

  override def newFeatureDataInstance() = new EncodedPixelYearCount()
}

/**
  * A structure that stores counts by year for pixels in a highly optimised manner.
  */
class EncodedPixelYearCount extends FeatureData[YearCount] {
  val data = MMap[EncodedPixelYear, Int]()

  /**
    * @return An iterator presenting the features per pixel
    */
  override def iterator() : Iterator[(Pixel, YearCount)] = {
    data.toIterator.map(e => {
      val (pixelYear, count) = e
      val (encodedPixel, year) = MapUtils.decodePixelYear(pixelYear)
      val pixel = MapUtils.decodePixel(encodedPixel)
      (pixel, new YearCount(year, count))
    })
  }

  override def collect(pixel: Pixel, feature: YearCount)  = {
    val encodedPixel = MapUtils.encodePixel(pixel)
    val encodedPixelYear = MapUtils.encodePixelYear(encodedPixel, feature.year)
    data(encodedPixelYear) = data.getOrElse(encodedPixelYear, 0) + feature.count
  }

}

/**
  * Defines the interface that must be implemented by tiles to present their features in a typed manner.
  * @tparam FT The content type stored per pixel
  */
trait FeatureData[FT] {
  def iterator(): Iterator[(Pixel, FT)]
  def collect(pixel: Pixel, feature: FT)
}
