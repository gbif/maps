package org.gbif.maps.spark

import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable.{Map => MMap}

/**
  * A DensityTile represents a classical tile of data which contains a count per year per basisOfRecord for each
  * pixel of the tile.
  */
class DensityTile() {
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
  def collect(basisOfRecord: Feature.BasisOfRecord, pixelData: MMap[EncodedPixelYear,Int]) = {
    val tilePixels = getData().getOrElseUpdate(basisOfRecord, MMap[EncodedPixelYear, Int]())
    pixelData.foreach(feature => {
      tilePixels(feature._1) = tilePixels.getOrElse(feature._1, 0) + feature._2
    })
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
  def getBufferRegion(region: DensityTile.Region.Value, tileSize: Int, bufferSize: Int): DensityTile = {
    // convience container describing the adjustment to make
    case class Adjustment (minX: Int, minY: Int, maxX: Int, maxY: Int, offsetX: Int, offsetY: Int)

    val adjustment = region match {
      case DensityTile.Region.N => new Adjustment(0, 0, tileSize, bufferSize, 0, tileSize)
      case DensityTile.Region.NE => new Adjustment(tileSize-bufferSize, 0, tileSize, bufferSize, -tileSize, tileSize)
      case DensityTile.Region.E => new Adjustment(tileSize-bufferSize, 0, tileSize, tileSize, -tileSize, 0)
      case DensityTile.Region.SE => new Adjustment(tileSize-bufferSize, tileSize-bufferSize, tileSize, tileSize, -tileSize, -tileSize)
      case DensityTile.Region.S => new Adjustment(0, tileSize-bufferSize, tileSize, tileSize, 0, -tileSize)
      case DensityTile.Region.SW => new Adjustment(0, tileSize-bufferSize, bufferSize, tileSize, tileSize, -tileSize)
      case DensityTile.Region.W => new Adjustment(0, 0, bufferSize, tileSize, tileSize, 0)
      case DensityTile.Region.NW => new Adjustment(0, 0, bufferSize, bufferSize, tileSize, tileSize)
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
}

/**
  * "Static methods" on DensityTile
  */
object DensityTile {
  // Enum for North, South, East etc.
  object Region extends Enumeration {
    val N, S, E, W, NE, NW, SE, SW = Value
  }

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
