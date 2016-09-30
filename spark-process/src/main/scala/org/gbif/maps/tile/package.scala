package org.gbif.maps

/**
  * Provides classes for building pyramids of tiles.
  */
package object tile {

  // A pixel address container
  case class Pixel (x: Short, y: Short)

  // A tile address container
  case class ZXY (z: Int, x: Long, y: Long) extends Ordered[ZXY] {
    override def toString : String = z + ":" + x + ":" + y
    def compare(that: ZXY): Int =  this.toString compare that.toString
  }

  // A year and count container
  case class YearCount(year: Year, count: Int)

  // Aliases for primitives types to aid readability when using tuples
  type EncodedPixel = Int
  type EncodedPixelYear = Long
  type Year = Short

  // Encodes the Pixel into an EncodedPixel
  def encodePixel(p: Pixel) : EncodedPixel = {
    p.x << 16 | p.y & 0xFFFF
  }

  // Decodes the EncodedPixel into a Pixel
  def decodePixel(encoded: EncodedPixel) : Pixel = {
    Pixel((encoded >> 16).asInstanceOf[Short], (encoded & 0xFFFF).asInstanceOf[Short])
  }

  // Encodes an EncodedPixel and Year into a Long
  def encodePixelYear(p: EncodedPixel, year: Year) : EncodedPixelYear = {
    p.toLong << 32 | year & 0xFFFF
  }

  // Decodes an EncodedPixelYear into an EncodedPixel and Year pair
  def decodePixelYear(py: EncodedPixelYear) : (EncodedPixel, Year) = {
    ((py >> 32).asInstanceOf[Int], (py & 0xFFFF).asInstanceOf[Short])
  }
}
