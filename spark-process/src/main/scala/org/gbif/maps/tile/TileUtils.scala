package org.gbif.maps.tile

import scala.collection.mutable.{Map => MMap, Set => MSet}

/**
  * Utilities for dealing with tiles.
  */
private[tile] object TileUtils {

  /**
    * A region (North, South etc) of a tile.  The value TILE is used to represent the complete tile canvas.
    *
    */
  object Region extends Enumeration {
    val N, S, E, W, NE, NW, SE, SW, TILE = Value
  }

  /**
    * Returns the regions of a tile which are of interest when building buffers.  This will always return the region
    * representing the tile itself (TILE) and the regions which overlap with the buffers of the adjacent tiles.
    * <p/>
    * Identifying the regions is of critical importance because it dictates which pixels which should be considered in
    * adjacent tiles when stitching together buffers.  The allows the caller to indicate they are also downscaling the
    * pixels from one zoom to a zoom 1 level higher which is a performance optimisation (can be done in a single pass
    * of the source tile).
    *
    * @param zxy The tile address
    * @param downscale If the tile is being downscaled at the same time as buffering
    * @param wrapDateline True if the dateline should wrap
    * @return The regions that are of interest
    */
  def bufferRegions(zxy: ZXY, downscale: Boolean , wrapDateline: Boolean) : Set[Region.Value] = {
    // initialise the result, knowing that our own canvas is always of interest
    val result = MSet[Region.Value](Region.TILE)

    if (downscale) {
      // if we are downscaling then only certain zones are possible as others lie in the center of the target tile
      (zxy.x % 2, zxy.y % 2) match {
        case (0, 0) => result ++= Set(Region.N, Region.W, Region.NW)
        case (1, 0) => result ++= Set(Region.N, Region.E, Region.NE)
        case (0, 1) => result ++= Set(Region.S, Region.W, Region.SW)
        case (1, 1) => result ++= Set(Region.S, Region.E, Region.SE)
        case _ => ; // ignore others (like -1,0)
      }
    } else {
      // all regions may be of interest
      result ++= Set(Region.N, Region.NE, Region.E, Region.SE,
        Region.S, Region.SW, Region.W, Region.NW)
    }

    val targetZ = if (downscale) zxy.z - 1 else zxy.z
    val maxTileAddress = maxTileAddressForZoom(targetZ)
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

    // special case for when z was already 0, not covered by the rules above
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
  def adjustPixelForBuffer(pixel: Pixel, region: Region.Value, tileSize: Int, bufferSize: Int) : Option[Pixel] = {
    // convience container describing the filter to test and the adjustment to make
    case class Adjustment (minX: Int, minY: Int, maxX: Int, maxY: Int, offsetX: Int, offsetY: Int)

    // Depending on the region we apply different adjustments to the pixel location.  E.g. a pixel destined to the
    // tile directly above, needs the pixel Y address offset to the reference of that tile, which is a 1 x tileSize
    // adjustment
    val adjustment = region match {
      case Region.TILE => new Adjustment(0, 0, tileSize, tileSize, 0, 0) // no adjustment
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

  /**
    * Returns the tile address for the adjacent tile in the direction specified.  Dateline handling is supported by
    * wrapping, but if a direction is requested North or South that goes beyond the poles then addresses that are out
    * of bounds will be returned.  Developers should safeguard against this beforehand (Note: bufferRegions does that
    * for you and is highly encouraged).
    *
    * @param zxy For the current tile
    * @param direction To indicate which adjacent tile is being requested
    * @return The address of the tile adjacent to the current tile in the direction specified
    */
  def adjacentTileZXY(zxy:ZXY, direction: Region.Value): ZXY = {
    val address = direction match {
      case Region.TILE => zxy
      case Region.N => new ZXY(zxy.z, zxy.x, zxy.y - 1)
      case Region.NE => new ZXY(zxy.z, zxy.x + 1, zxy.y - 1)
      case Region.E => new ZXY(zxy.z, zxy.x + 1, zxy.y)
      case Region.SE => new ZXY(zxy.z, zxy.x + 1, zxy.y + 1)
      case Region.S => new ZXY(zxy.z, zxy.x, zxy.y + 1)
      case Region.SW => new ZXY(zxy.z, zxy.x - 1, zxy.y + 1)
      case Region.W => new ZXY(zxy.z, zxy.x - 1, zxy.y)
      case Region.NW => new ZXY(zxy.z, zxy.x - 1, zxy.y - 1)
    }

    // handle wrapping across the dateline
    val maxTileAddress = maxTileAddressForZoom(zxy.z)
    val x = address.x match {
      case t if t>maxTileAddress => 0
      case t if t<0 => maxTileAddress
      case _ => address.x
    }

    new ZXY(address.z, x, address.y)
  }

  /**
    * Returns the maximum tile X or Y address for the given zoom.
    * @param zoom The zoom level
    * @return The maximum address on the grid
    */
  def maxTileAddressForZoom(zoom:Int) : Long= {
    (1 << zoom) - 1
  }

  /**
    * Adjusts the pixel location for use in a tile 1 zoom lower if required.
    *
    * @param pixel To downscale
    * @param downscale If true adjusts the pixel, otherwise this method returns the input pixel
    * @return The downscaled pixel or input pixel
    */
  def downscalePixel(zxy:ZXY, tileSize: Int, pixel: Pixel, downscale: Boolean) : Pixel = {
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
    * Adjusts the address given to be used for one zoom lower if required.
    *
    * @param zxy To downscale
    * @param downscale If true the address is adjusted, otherwise this method returns the input address
    * @return The downscaled address or the input address
    */
  def downscaleAddress(zxy: ZXY, downscale: Boolean) : ZXY = {
    if (downscale) {
      return ZXY(zxy.z-1, zxy.x/2, zxy.y/2)
    }
    return zxy
  }
}

