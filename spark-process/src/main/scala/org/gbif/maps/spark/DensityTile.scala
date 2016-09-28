package org.gbif.maps.spark

import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{Set => MSet}


/**
  * A DensityTile represents a classical tile of data which contains a count per year per basisOfRecord for each
  * pixel of the tile.
  */
class DensityTile() extends Serializable {
  // the pixel x,y can be encoded into an Int
  private type EncodedPixel = Int

  // the pixel and year can be encoded into a Long
  private type EncodedPixelYear = Long

  // the internal structure of data, optimised using encoded values
  private val data = MMap[Feature.BasisOfRecord, MMap[EncodedPixelYear,Int]]()

  /**
    * @return the internal data which is mutable
    */
  def getData() : MMap[Feature.BasisOfRecord, MMap[EncodedPixelYear,Int]] = {
    data
  }

  /**
    * Collects the provided features into this tile, merging the counts if required.
    * @param features To collect into this tile
    */
  def collect(basisOfRecord: Feature.BasisOfRecord, pixelData: MMap[EncodedPixelYear,Int]) : DensityTile = {
    val tilePixels = getData().getOrElseUpdate(basisOfRecord, MMap[EncodedPixelYear, Int]())
    pixelData.foreach(feature => {
      tilePixels(feature._1) = tilePixels.getOrElse(feature._1, 0) + feature._2
    })
    this
  }

  /**
    * Returns a new tile containing only the pixels within the named region.  The pixel coordinates in the resulting
    * tile are readdressed to be used as the buffer region for the adjacent tile.
    * <p/>
    * For example, the North region of this tile contains features that will lie in the southern buffer region of the
    * tile directly above the current tile.  However, while they will have very low Y addresses in this tile, in the
    * tile above, they will at "tileSize + currentAddressY".  This method makes that adjustment of addressing.
    *
    * @param region Of interest within the boundaries of the current tile
    * @param tileSize That we are working on
    * @param bufferSize The defines the depth of the buffer
    * @return A new tile, containing features suitable for use as a buffer in an adjacent tile
    */
  def getBufferRegion(region: TileBuffers.Region.Value, tileSize: Int, bufferSize: Int): DensityTile = {
    // convience container describing the adjustment to make
    case class Adjustment (minX: Int, minY: Int, maxX: Int, maxY: Int, offsetX: Int, offsetY: Int)

    val adjustment = region match {
      case TileBuffers.Region.N => new Adjustment(0, 0, tileSize, bufferSize, 0, tileSize)
      case TileBuffers.Region.NE => new Adjustment(tileSize-bufferSize, 0, tileSize, bufferSize, -tileSize, tileSize)
      case TileBuffers.Region.E => new Adjustment(tileSize-bufferSize, 0, tileSize, tileSize, -tileSize, 0)
      case TileBuffers.Region.SE => new Adjustment(tileSize-bufferSize, tileSize-bufferSize, tileSize, tileSize, -tileSize, -tileSize)
      case TileBuffers.Region.S => new Adjustment(0, tileSize-bufferSize, tileSize, tileSize, 0, -tileSize)
      case TileBuffers.Region.SW => new Adjustment(0, tileSize-bufferSize, bufferSize, tileSize, tileSize, -tileSize)
      case TileBuffers.Region.W => new Adjustment(0, 0, bufferSize, tileSize, tileSize, 0)
      case TileBuffers.Region.NW => new Adjustment(0, 0, bufferSize, bufferSize, tileSize, tileSize)
    }

    val target = new DensityTile()
    getData().foreach(e => {
      // for each EncodedPixelYear feature accumulate counts
      val bor = target.getData().getOrElseUpdate(e._1, MMap[EncodedPixelYear, Int]())
      e._2.foreach(f => {
        val pixelYear = MapUtils.decodePixelYear(f._1)
        val pixel = MapUtils.decodePixel(pixelYear._1)
        val year = pixelYear._2
        val count = f._2

        // if the pixel falls within our area of interest, adjust the coordinates and add the contents to the target
        if (pixel.x >= adjustment.minX && pixel.x < adjustment.maxX &&
            pixel.y >= adjustment.minY && pixel.y < adjustment.maxY) {

          val newPixel = MapUtils.encodePixel(
            new Pixel((pixel.x.asInstanceOf[Int] + adjustment.offsetX).asInstanceOf[Short],
              (pixel.y.asInstanceOf[Int] + adjustment.offsetY).asInstanceOf[Short]));

          val newPixelYear = MapUtils.encodePixelYear(newPixel, year)
          bor(newPixelYear) = bor.getOrElse(newPixelYear, 0) + count
        }
      })
    })
    target
  }

  /**
    * Returns a new DensityTile prepared to be used as a quadrant in another DensityTile at one zoom level lower.
    * This is intended to be used in algorithms where e.g. the 4 tiles at zoom level one can each be downscaled
    * independantly and then merged into a single tile to represent zoom 0.
    * When downscaling the pixel addresses are adjusted ready for use in the new zoom level.
    */
  def downscale(x: Long, y: Long, tileSize: Int): DensityTile = {
    val target = new DensityTile()

    getData().foreach(f => {
      val bor = target.getData().getOrElseUpdate(f._1, MMap[Long,Int]())

      f._2.foreach(feature => {
        val (encPixel, year) = MapUtils.decodePixelYear(feature._1)
        val pixel = MapUtils.decodePixel(encPixel)

        // Important(!)
        // We only emit pixels that fall on the current tile and exclude any that are in it's buffer.
        // If an e.g. eastzone buffer pixel of x=260 on a 256px tile  goes through the following code it would be
        // painted at 130px incorrectly.  We discard buffered pixels here as buffers are recreated if needed
        // after the tiles are downscaled.
        if (pixel.x >= 0 && pixel.x < tileSize &&
            pixel.y >= 0 && pixel.y < tileSize) {

          // identify the quadrant it falls in, and adjust the pixel address accordingly
          val px = (pixel.x/2 + tileSize/2 * (x%2)).asInstanceOf[Short]
          val py = (pixel.y/2 + tileSize/2 * (y%2)).asInstanceOf[Short]

          val newKey = MapUtils.encodePixelYear(MapUtils.encodePixel(Pixel(px,py)), year)
          bor(newKey) = bor.getOrElse(newKey, 0) + feature._2

        }
      })
    })
    target
  }

  /**
    * Returns a new DensityTile prepared to be used as a quadrant in another DensityTile at one zoom level lower.
    * This is intended to be used in algorithms where e.g. the 4 tiles at zoom level one can each be downscaled
    * independantly and then merged into a single tile to represent zoom 0.
    * When downscaling the pixel addresses are adjusted ready for use in the new zoom level.
    */
  def downscaleBuffer(x: Long, y: Long, tileSize: Int, bufferSize: Int, downscale: Boolean): MMap[TileBuffers.Region.Value, DensityTile] = {
    val result = MMap[TileBuffers.Region.Value, DensityTile]()

    // iterate over all layers of data and downscale the content by pixel.
    getData().foreach(layer => {
      val basisOfRecord = layer._1

      // feature representing the combination of a pixel+year with a count
      layer._2.foreach(feature => {

        val (encPixel, year) = MapUtils.decodePixelYear(feature._1)
        val pixel = MapUtils.decodePixel(encPixel)
        val count = feature._2

        // We only care about pixels that fall on the current tile and exclude any that are in our buffer otherwise
        // things get double counted as those pixels will be treated on their true tile.
        if (pixel.x >= 0 && pixel.x < tileSize &&
            pixel.y >= 0 && pixel.y < tileSize) {

          var newPixel = pixel
          if (downscale) {
            // identify the quadrant the current tile falls in and adjust the pixel address accordingly ready to be used
            // in the tile it will fall in at one zoom lower
            val px = (pixel.x/2 + tileSize/2 * (x%2)).asInstanceOf[Short]
            val py = (pixel.y/2 + tileSize/2 * (y%2)).asInstanceOf[Short]
            val newPixel = Pixel(px,py)
          }

          val regionsOfInterest : Set[TileBuffers.Region.Value] = getRegionsOfInterest(x,y)
          regionsOfInterest.foreach(region =>
            appendPixel(result, region, basisOfRecord, newPixel, year, count, tileSize, bufferSize)
          )
        }
      })
    })
    result
  }

  /**
    * Converts this tile into a collection of new tiles each containing the pixels that will fall in buffer zones of
    * tiles adjacent to the this tile. In the emited tiles the pixels are re-addressed to be relative to the origin
    * for the tile in which they will fall in a buffer.  For example, when running with a bufferSize of 10 a pixel at
    * x=5 on the current tile will reside in the eastern buffer of the tile immediately to the west at an address of
    * x=tileSize+5.
    * <p/>
    * the emitted collection is then suitable for stitching together to produce tiles with buffer zones (i.e. data
    * which is off-canvas).
    * <p/>
    * Optionally, this can downscale the pixel addresses by 1 zoom level, so the resulting
    * tiles can be used at a lower zoom. Combining the optional downscaling in this method is a performance
    * optimisation as only a single scan of this tile is then needed when building buffered tiles for the lower zoom
    * level .
    *
    * @param x
    * @param y
    * @param tileSize
    * @param bufferSize
    * @param downscale
    * @return
    */
  def flatMapToRegions(regionsOfInterest : Set[TileBuffers.Region.Value], x: Long, y: Long, tileSize: Int,
    bufferSize: Int, downscale: Boolean): MMap[TileBuffers.Region.Value, DensityTile] = {
    val result = MMap[TileBuffers.Region.Value, DensityTile]()

    getData().foreach(layer => {
      val basisOfRecord = layer._1

      // feature representing the combination of a pixel+year with a count
      layer._2.foreach(feature => {

        val (encPixel, year) = MapUtils.decodePixelYear(feature._1)
        val pixel = MapUtils.decodePixel(encPixel)
        val count = feature._2

        // Only pixels on our canvas are of interest to void double counting
        if (pixel.x >= 0 && pixel.x < tileSize && pixel.y >= 0 && pixel.y < tileSize) {

          // recode the pixel address if downscaling
          var newPixel = pixel
          if (downscale) {
            // identify the quadrant the current tile falls in and adjust the pixel address accordingly ready to be used
            // in the tile it will fall in at one zoom lower
            val px = (pixel.x/2 + tileSize/2 * (x%2)).asInstanceOf[Short]
            val py = (pixel.y/2 + tileSize/2 * (y%2)).asInstanceOf[Short]
            newPixel = Pixel(px,py)
          }

          regionsOfInterest.foreach(region =>
            appendPixel(result, region, basisOfRecord, newPixel, year, count, tileSize, bufferSize)
          )
        }
      })
    })
    result
  }

  /**
    * Appends a pixel of data to a tile in the target after applying the given offsets.  The target tile is located
    * by a region (e.g. North) which should correspond with the
    *
    * @param target
    * @param region
    * @param basisOfRecord
    * @param px
    * @param py
    * @param year
    * @param count
    * @param offsetX
    * @param offsetY
    */
  private def appendPixel(context: MMap[TileBuffers.Region.Value, DensityTile], region: TileBuffers.Region.Value,
    basisOfRecord: Feature.BasisOfRecord, pixel: Pixel, year: Year, count: Int,
    tileSize: Int, bufferSize: Int
  ) : Unit = {
    // convience container describing the adjustment to make
    case class Adjustment (minX: Int, minY: Int, maxX: Int, maxY: Int, offsetX: Int, offsetY: Int)

    // Depending on the region we apply different adjustments to the pixel location.  E.g. a pixel destined to the
    // tile directly above, needs the pixel Y address offset to the reference of that tile, which is a 1 x tileSize
    // adjustment
    val adjustment = region match {
      case TileBuffers.Region.CENTER => new Adjustment(0, 0, tileSize, tileSize, 0, 0)
      case TileBuffers.Region.N => new Adjustment(0, 0, tileSize, bufferSize, 0, tileSize)
      case TileBuffers.Region.NE => new Adjustment(tileSize-bufferSize, 0, tileSize, bufferSize, -tileSize, tileSize)
      case TileBuffers.Region.E => new Adjustment(tileSize-bufferSize, 0, tileSize, tileSize, -tileSize, 0)
      case TileBuffers.Region.SE => new Adjustment(tileSize-bufferSize, tileSize-bufferSize, tileSize, tileSize, -tileSize, -tileSize)
      case TileBuffers.Region.S => new Adjustment(0, tileSize-bufferSize, tileSize, tileSize, 0, -tileSize)
      case TileBuffers.Region.SW => new Adjustment(0, tileSize-bufferSize, bufferSize, tileSize, tileSize, -tileSize)
      case TileBuffers.Region.W => new Adjustment(0, 0, bufferSize, tileSize, tileSize, 0)
      case TileBuffers.Region.NW => new Adjustment(0, 0, bufferSize, bufferSize, tileSize, tileSize)
    }

    // only if the pixel falls within our area of interest, adjust the coordinates and add the contents to the target
    if (pixel.x >= adjustment.minX && pixel.x < adjustment.maxX &&
        pixel.y >= adjustment.minY && pixel.y < adjustment.maxY) {

      val newPixel = MapUtils.encodePixel(
        new Pixel((pixel.x.asInstanceOf[Int] + adjustment.offsetX).asInstanceOf[Short],
          (pixel.y.asInstanceOf[Int] + adjustment.offsetY).asInstanceOf[Short]));

      val newPixelYear = MapUtils.encodePixelYear(newPixel, year)
      val targetTile = context.getOrElseUpdate(region, new DensityTile())
      val bor = targetTile.getData().getOrElseUpdate(basisOfRecord, MMap[EncodedPixelYear,Int]())
      bor(newPixelYear) = bor.getOrElse(newPixelYear, 0) + count
    }
  }

  /**
    * Utility to determine which quadrants of a
    * @param x
    * @param y
    * @return
    */
  private def getRegionsOfInterest(x: Long, y:Long) : Set[TileBuffers.Region.Value] = {
    val quadrant = (x%2,y%2) match {
      case (0,0) => Set(TileBuffers.Region.CENTER, TileBuffers.Region.N, TileBuffers.Region.W, TileBuffers.Region.NW)
      case (1,0) => Set(TileBuffers.Region.CENTER, TileBuffers.Region.N, TileBuffers.Region.E, TileBuffers.Region.NE)
      case (0,1) => Set(TileBuffers.Region.CENTER, TileBuffers.Region.S, TileBuffers.Region.W, TileBuffers.Region.SW)
      case (1,1) => Set(TileBuffers.Region.CENTER, TileBuffers.Region.S, TileBuffers.Region.E, TileBuffers.Region.SE)
    }
    quadrant
  }


  def isEmpty() : Boolean = {
    return getData().isEmpty
  }
}

/**
  * "Static methods" on DensityTile
  */
object DensityTile {

  /**
    * Merges the provided tiles into a new instance without mutating the sources
    *
    * @param sources The source tiles to merge
    * @return A new instance
    */
  def merge(sources: DensityTile*) : DensityTile = {
    val target = new DensityTile()
    sources.foreach(source => {
      source.getData().foreach(e => {
        val bor = target.getData().getOrElseUpdate(e._1, MMap[EncodedPixelYear,Int]())
        // for each EncodedPixelYear feature accumulate counts
        e._2.foreach( f => {
          bor(f._1) = bor.getOrElse(f._1, 0) + f._2
        })
      })
    })
    target
  }
}

object TileBuffers {
  object Region extends Enumeration {
    val N, S, E, W, NE, NW, SE, SW, CENTER = Value
  }

  /**
    * Returns the regions of the tile that will overlap with buffer zone of an adjacent tile.  This is useful when
    * stitching together tiles to produce tiles with buffers.
    * @param z The current zoom level
    * @param x The tile x address
    * @param y The tile y address
    * @param tileSize The tile size in pixels
    * @param downscale If this is downscaling at the same time as buffering
    * @param datelineWrapping If the eastern and western hemispheres should wrap and buffer
    * @return The areas of interest
    */
  def bufferRegions(z: Int, x: Long, y:Long, tileSize: Int, downscale: Boolean , datelineWrapping: Boolean) : Set[TileBuffers.Region.Value] = {
    // initialise the result, knowing that our own canvas is always of interest
    val result = MSet[TileBuffers.Region.Value](TileBuffers.Region.CENTER)

    if (downscale) {
      // if we are downscaling then only the zones which will be
      (x%2,y%2) match {
        case (0,0) => result ++= Set(TileBuffers.Region.N, TileBuffers.Region.W, TileBuffers.Region.NW)
        case (1,0) => result ++= Set(TileBuffers.Region.N, TileBuffers.Region.E, TileBuffers.Region.NE)
        case (0,1) => result ++= Set(TileBuffers.Region.S, TileBuffers.Region.W, TileBuffers.Region.SW)
        case (1,1) => result ++= Set(TileBuffers.Region.S, TileBuffers.Region.E, TileBuffers.Region.SE)
      }
    }  else {
      // all regions may be of interest
      result ++= Set(TileBuffers.Region.N, TileBuffers.Region.NE, TileBuffers.Region.E, TileBuffers.Region.SE,
        TileBuffers.Region.S, TileBuffers.Region.SW, TileBuffers.Region.W, TileBuffers.Region.NW)
    }

    val targetZ = if (downscale) z-1 else z
    val maxTileAddress = if (targetZ==0) 0 else (2 << (targetZ-1)) - 1
    val targetX = if (downscale) x>>>1 else x
    val targetY = if (downscale) y>>>1 else y

    // trim to the world handling dateline wrapping if desirable
    if (targetY == 0) {
      result -= (TileBuffers.Region.N, TileBuffers.Region.NE, TileBuffers.Region.NW) // nothing north of us
    }
    if (targetY == maxTileAddress) {
      result -= (TileBuffers.Region.S, TileBuffers.Region.SE, TileBuffers.Region.SW) // nothing south of us
    }
    if (targetX == 0 && !datelineWrapping) {
      result -= (TileBuffers.Region.NW, TileBuffers.Region.W, TileBuffers.Region.SW) // nothing west of us
    }
    if (targetX == maxTileAddress && !datelineWrapping) {
      result -= (TileBuffers.Region.NE, TileBuffers.Region.E, TileBuffers.Region.SE) // nothing west of us
    }

    // this is a special case, not covered by the rules above
    if (z == 0 && datelineWrapping) {
      result ++= Set(TileBuffers.Region.E, TileBuffers.Region.W)
    }
    return result.toSet
  }

}

