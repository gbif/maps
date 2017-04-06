package org.gbif.maps.tile

import org.scalatest.FunSuite
import scala.collection.mutable.{HashSet => MHashSet}

/**
  * Unit tests for Tile package functions and generic operations on the Tile class.
  */
class TileSuite extends FunSuite {
  // utility container for a pixel on a tile
  private case class TilePixelCount(z: Int, x: Long, y: Long, px: Int, py: Int, count: Int)

  test("Ensure pixels encode and decode correctly") {
    assert(Pixel(0,0) === encodeDecode(Pixel(0,0)))
    assert(Pixel(0,-1) === encodeDecode(Pixel(0,-1)))
    assert(Pixel(10,10) === encodeDecode(Pixel(10,10)))
    assert(Pixel(-16,16) === encodeDecode(Pixel(-16,16)))
    assert(Pixel(16,-1) === encodeDecode(Pixel(16,-1)))
    assert(Pixel(-43,-43) === encodeDecode(Pixel(-43,-43)))
  }

  test("Ensure the pixel and year encode and decode correctly") {
    assert((Pixel(0,0), 1970) === encodeDecode(Pixel(0,0), 1970.asInstanceOf[Short]))
    assert((Pixel(0,0), -1970) === encodeDecode(Pixel(0,0), (-1970).asInstanceOf[Short]))
    assert((Pixel(0,0), -1) === encodeDecode(Pixel(0,0), (-1).asInstanceOf[Short]))

    assert((Pixel(0,-1), 1970) === encodeDecode(Pixel(0,-1), 1970.asInstanceOf[Short]))
    assert((Pixel(0,-1), -1970) === encodeDecode(Pixel(0,-1), (-1970).asInstanceOf[Short]))
    assert((Pixel(0,-1), -1) === encodeDecode(Pixel(0,-1), (-1).asInstanceOf[Short]))

    assert((Pixel(10,10), 1970) === encodeDecode(Pixel(10,10), 1970.asInstanceOf[Short]))
    assert((Pixel(10,10), -1970) === encodeDecode(Pixel(10,10), (-1970).asInstanceOf[Short]))
    assert((Pixel(10,10), -1) === encodeDecode(Pixel(10,10), (-1).asInstanceOf[Short]))

    assert((Pixel(-16,16), 1970) === encodeDecode(Pixel(-16,16), 1970.asInstanceOf[Short]))
    assert((Pixel(-16,16), -1970) === encodeDecode(Pixel(-16,16), (-1970).asInstanceOf[Short]))
    assert((Pixel(-16,16), -1) === encodeDecode(Pixel(-16,16), (-1).asInstanceOf[Short]))

    assert((Pixel(16,-1), 1970) === encodeDecode(Pixel(16,-1), 1970.asInstanceOf[Short]))
    assert((Pixel(16,-1), -1970) === encodeDecode(Pixel(16,-1), (-1970).asInstanceOf[Short]))
    assert((Pixel(16,-1), -1) === encodeDecode(Pixel(16,-1), (-1).asInstanceOf[Short]))

    assert((Pixel(-43,-43), 1970) === encodeDecode(Pixel(-43,-43), 1970.asInstanceOf[Short]))
    assert((Pixel(-43,-43), -1970) === encodeDecode(Pixel(-43,-43), (-1970).asInstanceOf[Short]))
    assert((Pixel(-43,-43), -1) === encodeDecode(Pixel(-43,-43), (-1).asInstanceOf[Short]))
  }

  /**
    * Specific test to check that pixels in the far western area are placed into eastern border buffers when
    * downscaling.
    */
  test("Ensure dateline handling with downscaling for westerly pixels") {
    val layerName = "test"

    // a western tile at z16, with a pixel that lies in the NW region
    val sourceTile = new SingleChannelRasterTile(new ZXY(16, 0, 10))
    sourceTile.collect(layerName, new Pixel(1,1), 100)

    val downscaled = sourceTile.flatMapToBuffers(true, true)
    val pixelData = toFullPixelAddress(downscaled)
    assertResult(4)(pixelData.size)
    assert(pixelData.contains(new TilePixelCount(15,0,5,0,0,100))) // tile center
    assert(pixelData.contains(new TilePixelCount(15,0,4,0,512,100))) // N tile
    assert(pixelData.contains(new TilePixelCount(15,32767,5,512,0,100))) // W tile wrapping across dateline
    assert(pixelData.contains(new TilePixelCount(15,32767,4,512,512,100))) // NW tile wrapping across dateline
  }

  // Extract all pixels at a full address (tile and pixel)
  private def toFullPixelAddress(tiles: Iterable[Tile[String, Int]]) : Set[TilePixelCount] = {
    val data = new MHashSet[TilePixelCount]()
    for (tile <- tiles) {
      val zxy = tile.getZXY()
      for ((layer,features) <- tile.getData()) {
        for ((pixel,count) <- features.iterator()) {
          data += (new TilePixelCount(zxy.z, zxy.x, zxy.y, pixel.x, pixel.y, count))
        }
      }
    }
    data.toSet
  }

  private def encodeDecode(pixel: Pixel) : Pixel = {
    val encoded = encodePixel(pixel)
    decodePixel(encoded)
  }

  private def encodeDecode(pixel: Pixel, year: Year) : (Pixel, Year) = {
    val encoded = encodePixelYear(encodePixel(pixel), year)
    val decoded = decodePixelYear(encoded)
    (decodePixel(decoded._1), decoded._2)
  }
}
