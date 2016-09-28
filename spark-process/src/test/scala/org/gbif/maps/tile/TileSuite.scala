package org.gbif.maps.tile

import org.gbif.maps.spark._
import org.scalatest.FunSuite


/**
  * Unit tests for Tile package functions.
  */
class TileSuite extends FunSuite {

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

  private def encodeDecode(pixel: Pixel) : Pixel = {
    var encoded = encodePixel(pixel)
    decodePixel(encoded)
  }

  private def encodeDecode(pixel: Pixel, year: Year) : (Pixel, Year) = {
    var encoded = encodePixelYear(encodePixel(pixel), year)

    var decoded = decodePixelYear(encoded)
    (decodePixel(decoded._1), decoded._2)
  }
}
