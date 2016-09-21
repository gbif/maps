package org.gbif.maps.spark

import org.gbif.maps.spark._
import org.scalatest.FunSuite

/**
  * Unit tests for MapUtils.
  */
class MapUtilsSuite extends FunSuite {
  test("pixel encoding is corrupt") {
    assert(Pixel(0,0) === encodeDecode(Pixel(0,0)))
    assert(Pixel(0,-1) === encodeDecode(Pixel(0,-1)))
    assert(Pixel(10,10) === encodeDecode(Pixel(10,10)))
    assert(Pixel(-16,16) === encodeDecode(Pixel(-16,16)))
    assert(Pixel(16,-1) === encodeDecode(Pixel(16,-1)))
    assert(Pixel(-43,-43) === encodeDecode(Pixel(-43,-43)))
  }

  test("pixelYear encoding is corrupt") {
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


  private def encodeDecode(pixel: Pixel) : Pixel = {
    var encoded = MapUtils.encodePixel(pixel)
    MapUtils.decodePixel(encoded)
  }

  private def encodeDecode(pixel: Pixel, year: Year) : (Pixel, Year) = {
    var encoded = MapUtils.encodePixelYear(
      MapUtils.encodePixel(pixel), year)

    var decoded = MapUtils.decodePixelYear(encoded)
    (MapUtils.decodePixel(decoded._1), decoded._2)
  }
}
