package org.gbif.maps.spark

import org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord
import org.scalatest.FunSuite

import scala.collection.mutable.{Map => MMap}

/**
  * Unit tests for DensityTile.
  */
class DensityTileSuite extends FunSuite {

  test("accumulation produced wrong number of features") {
    var tile = new DensityTile
    val pixels = MMap.empty[EncodedPixelYear,Int]

    pixels += (encode(10,10,1970) -> 10)
    pixels += (encode(10,10,1971) -> 20)
    pixels += (encode(10,11,1970) -> 30)
    tile.collect(BasisOfRecord.OBSERVATION, pixels)
    tile.collect(BasisOfRecord.HUMAN_OBSERVATION, pixels)
    tile.collect(BasisOfRecord.HUMAN_OBSERVATION, pixels) // collected twice

    assert(tile.getData().size === 2)
    var obs = tile.getData().get(BasisOfRecord.OBSERVATION)
    assert(obs.get.size === 3)
    assert(obs.get(encode(10,10,1970)) === 10)
    assert(obs.get(encode(10,10,1971)) === 20)
    assert(obs.get(encode(10,11,1970)) === 30)

    var humanObs = tile.getData().get(BasisOfRecord.HUMAN_OBSERVATION)
    assert(humanObs.get.size === 3)
    assert(humanObs.get(encode(10,10,1970)) === 20)
    assert(humanObs.get(encode(10,10,1971)) === 40)
    assert(humanObs.get(encode(10,11,1970)) === 60)
  }

  test("merging produced the wrong data") {
    var tile1 = new DensityTile
    val pixels1 = MMap.empty[EncodedPixelYear,Int]
    pixels1 += (encode(10,10,1970) -> 10)
    tile1.collect(BasisOfRecord.OBSERVATION, pixels1)

    var tile2 = new DensityTile
    val pixels2 = MMap.empty[EncodedPixelYear,Int]
    pixels2 += (encode(10,10,1970) -> 10) // same as tile1
    pixels2 += (encode(10,10,1971) -> 50)
    pixels2 += (encode(20,20,1971) -> 100)
    tile2.collect(BasisOfRecord.OBSERVATION, pixels2)

    var merged = DensityTile.merge(tile1, tile2)

    assert(merged.getData().size === 1) // 1 layer
    var obs = merged.getData().get(BasisOfRecord.OBSERVATION)
    assert(obs.get.size === 3)
    assert(obs.get(encode(10,10,1970)) === 20)
    assert(obs.get(encode(10,10,1971)) === 50)
    assert(obs.get(encode(20,20,1971)) === 100)
  }

  /**
    * Establish 2 perimeters of 8 points, one inside the buffer limits and the other further inside the tile, so that
    * they should not be considered for buffering in the adjacent tiles.
    * The tile is 512px, the buffer of 32 pixels.
    * Then test all regions return the points only in the buffer.
    */
  test("regions returning incorrectly") {
    var tile = new DensityTile
    val pixels = MMap.empty[EncodedPixelYear,Int]

    //assert(encode(16,-16,1970) !== encode(256,-16,1970))


    // perimeter within the bufferzone, starting at NW and running clockwise
    pixels += (encode(16,16,1970) -> 1)    // NW
    pixels += (encode(256,16,1970) -> 1)   // N
    pixels += (encode(496,16,1970) -> 1)   // NE
    pixels += (encode(496,256,1970) -> 1)  // E
    pixels += (encode(496,496,1970) -> 1)  // SE
    pixels += (encode(256,496,1970) -> 1)  // S
    pixels += (encode(16,496,1970) -> 1)   // SW
    pixels += (encode(16,256,1970) -> 1)   // W

    // perimeter within tile outside the bufferzone, starting at NW and running clockwise
    /*
    pixels += (encode(32,32,1970) -> 1)
    pixels += (encode(256,32,1970) -> 1)
    pixels += (encode(479,32,1970) -> 1)
    pixels += (encode(479,256,1970) -> 1)
    pixels += (encode(479,479,1970) -> 1)
    pixels += (encode(256,479,1970) -> 1)
    pixels += (encode(32,479,1970) -> 1)
    pixels += (encode(32,256,1970) -> 1)
    */

    tile.collect(BasisOfRecord.OBSERVATION, pixels)

    // North
    assertContains(
      tile.getBufferRegion(DensityTile.Region.N, 512, 32),
      3,
      BasisOfRecord.OBSERVATION,
      encode(16,528,1970),
      encode(256,528,1970),
      encode(496,528,1970)
    )

    // North East
    assertContains(
      tile.getBufferRegion(DensityTile.Region.NE, 512, 32),
      1,
      BasisOfRecord.OBSERVATION,
      encode(-16,528,1970)
    )

    // East
    assertContains(
      tile.getBufferRegion(DensityTile.Region.E, 512, 32),
      3,
      BasisOfRecord.OBSERVATION,
      encode(-16,16,1970),
      encode(-16,256,1970),
      encode(-16,496,1970)
    )

    // South East
    assertContains(
      tile.getBufferRegion(DensityTile.Region.SE, 512, 32),
      1,
      BasisOfRecord.OBSERVATION,
      encode(-16,-16,1970)
    )

    // South
    assertContains(
      tile.getBufferRegion(DensityTile.Region.S, 512, 32),
      3,
      BasisOfRecord.OBSERVATION,
      encode(16,-16,1970),
      encode(256,-16,1970),
      encode(496,-16,1970)
    )

  }

  /**
    * Utility to check that the provided tile contains the correct number of features, and the encoded feature keys are
    * present in the basisOfRecord layer.
    */
  private def assertContains(densityTile: DensityTile, totalFeatures: Int, basisOfRecord: BasisOfRecord, features: EncodedPixelYear*): Unit = {
    assert(densityTile.getData().get(basisOfRecord).get.size === totalFeatures)
    // ensure the map contains all the expected features
    features.foreach(f => {
      assert(densityTile.getData().get(basisOfRecord).get.contains(f))
    })
  }

  // enocde a pixel year easily to make tests more readable
  private def encode(x: Int, y: Int, year: Int) : EncodedPixelYear = {
    MapUtils.encodePixelYear(
      MapUtils.encodePixel(Pixel(x.asInstanceOf[Short],y.asInstanceOf[Short])),
      year.asInstanceOf[Short])
  }

}
