package org.gbif.maps.spark

import org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord
import org.scalatest.FunSuite

import scala.collection.mutable.{Map => MMap}
import TileBuffers.Region.{CENTER, N, NE, E, SE, S, SW, W, NW}

/**
  * Unit tests for DensityTile.
  */
class DensityTileSuite extends FunSuite {

  test("Verify the accumulation in features") {
    val tile = new DensityTile
    val pixels = MMap.empty[EncodedPixelYear,Int]

    pixels += (encode(10,10,1970) -> 10)
    pixels += (encode(10,10,1971) -> 20)
    pixels += (encode(10,11,1970) -> 30)
    tile.collect(BasisOfRecord.OBSERVATION, pixels)
    tile.collect(BasisOfRecord.HUMAN_OBSERVATION, pixels)
    tile.collect(BasisOfRecord.HUMAN_OBSERVATION, pixels) // collected twice

    assert(tile.getData().size === 2)
    val obs = tile.getData().get(BasisOfRecord.OBSERVATION)
    assert(obs.get.size === 3)
    assert(obs.get(encode(10,10,1970)) === 10)
    assert(obs.get(encode(10,10,1971)) === 20)
    assert(obs.get(encode(10,11,1970)) === 30)

    val humanObs = tile.getData().get(BasisOfRecord.HUMAN_OBSERVATION)
    assert(humanObs.get.size === 3)
    assert(humanObs.get(encode(10,10,1970)) === 20)
    assert(humanObs.get(encode(10,10,1971)) === 40)
    assert(humanObs.get(encode(10,11,1970)) === 60)
  }

  test("merging produced the wrong data") {
    val tile1 = new DensityTile
    val pixels1 = MMap.empty[EncodedPixelYear,Int]
    pixels1 += (encode(10,10,1970) -> 10)
    tile1.collect(BasisOfRecord.OBSERVATION, pixels1)

    val tile2 = new DensityTile
    val pixels2 = MMap.empty[EncodedPixelYear,Int]
    pixels2 += (encode(10,10,1970) -> 10) // same as tile1
    pixels2 += (encode(10,10,1971) -> 50)
    pixels2 += (encode(20,20,1971) -> 100)
    tile2.collect(BasisOfRecord.OBSERVATION, pixels2)

    val merged = DensityTile.merge(tile1, tile2)

    assert(merged.getData().size === 1) // 1 layer
    val obs = merged.getData().get(BasisOfRecord.OBSERVATION)
    assert(obs.get.size === 3)
    assert(obs.get(encode(10,10,1970)) === 20)
    assert(obs.get(encode(10,10,1971)) === 50)
    assert(obs.get(encode(20,20,1971)) === 100)
  }

  // this is critical path stuff so test all combinations that will be used
  test("Verify buffer regions") {
    // zoom 0
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(0, 0, 0, 512, false, false))
    assertResult(Set(CENTER, E, W)) (TileBuffers.bufferRegions(0, 0, 0, 512, false, true))

    // zoom 1, no downscaling, no wrapping
    assertResult(Set(CENTER, E, SE, S)) (TileBuffers.bufferRegions(1, 0, 0, 512, false, false))
    assertResult(Set(CENTER, W, SW, S)) (TileBuffers.bufferRegions(1, 1, 0, 512, false, false))
    assertResult(Set(CENTER, N, NE, E)) (TileBuffers.bufferRegions(1, 0, 1, 512, false, false))
    assertResult(Set(CENTER, W, NW, N)) (TileBuffers.bufferRegions(1, 1, 1, 512, false, false))

    // zoom 1, downscaling, no wrapping
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(1, 0, 0, 512, true, false))
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(1, 1, 0, 512, true, false))
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(1, 0, 1, 512, true, false))
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(1, 1, 1, 512, true, false))

    // zoom 1, downscaling, dateline wrapping
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(1, 0, 0, 512, true, true))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(1, 1, 0, 512, true, true))
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(1, 0, 1, 512, true, true))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(1, 1, 1, 512, true, true))

    // zoom 2, no downscaling, no wrapping
    assertResult(Set(CENTER, E, SE, S)) (TileBuffers.bufferRegions(2, 0, 0, 512, false, false))
    assertResult(Set(CENTER, W, SW, S, SE, E)) (TileBuffers.bufferRegions(2, 1, 0, 512, false, false))
    assertResult(Set(CENTER, W, SW, S, SE, E)) (TileBuffers.bufferRegions(2, 2, 0, 512, false, false))
    assertResult(Set(CENTER, W, SW, S)) (TileBuffers.bufferRegions(2, 3, 0, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, SE, S)) (TileBuffers.bufferRegions(2, 0, 1, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, SE, S, SW, W, NW)) (TileBuffers.bufferRegions(2, 1, 1, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, SE, S, SW, W, NW)) (TileBuffers.bufferRegions(2, 2, 1, 512, false, false))
    assertResult(Set(CENTER, N, S, SW, W, NW)) (TileBuffers.bufferRegions(2, 3, 1, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, SE, S)) (TileBuffers.bufferRegions(2, 0, 2, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, SE, S, SW, W, NW)) (TileBuffers.bufferRegions(2, 1, 2, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, SE, S, SW, W, NW)) (TileBuffers.bufferRegions(2, 2, 2, 512, false, false))
    assertResult(Set(CENTER, N, S, SW, W, NW)) (TileBuffers.bufferRegions(2, 3, 2, 512, false, false))
    assertResult(Set(CENTER, N, NE, E)) (TileBuffers.bufferRegions(2, 0, 3, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, W, NW)) (TileBuffers.bufferRegions(2, 1, 3, 512, false, false))
    assertResult(Set(CENTER, N, NE, E, W, NW)) (TileBuffers.bufferRegions(2, 2, 3, 512, false, false))
    assertResult(Set(CENTER, N, W, NW)) (TileBuffers.bufferRegions(2, 3, 3, 512, false, false))

    // zoom 2, downscaling, no wrapping
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(2, 0, 0, 512, true, false))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(2, 1, 0, 512, true, false))
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(2, 2, 0, 512, true, false))
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(2, 3, 0, 512, true, false))
    assertResult(Set(CENTER, S)) (TileBuffers.bufferRegions(2, 0, 1, 512, true, false))
    assertResult(Set(CENTER, E, SE, S)) (TileBuffers.bufferRegions(2, 1, 1, 512, true, false))
    assertResult(Set(CENTER, S, SW, W)) (TileBuffers.bufferRegions(2, 2, 1, 512, true, false))
    assertResult(Set(CENTER, S)) (TileBuffers.bufferRegions(2, 3, 1, 512, true, false))
    assertResult(Set(CENTER, N)) (TileBuffers.bufferRegions(2, 0, 2, 512, true, false))
    assertResult(Set(CENTER, N, NE, E)) (TileBuffers.bufferRegions(2, 1, 2, 512, true, false))
    assertResult(Set(CENTER, N, W, NW)) (TileBuffers.bufferRegions(2, 2, 2, 512, true, false))
    assertResult(Set(CENTER, N)) (TileBuffers.bufferRegions(2, 3, 2, 512, true, false))
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(2, 0, 3, 512, true, false))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(2, 1, 3, 512, true, false))
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(2, 2, 3, 512, true, false))
    assertResult(Set(CENTER)) (TileBuffers.bufferRegions(2, 3, 3, 512, true, false))

    // zoom 2, downscaling, wrapping
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(2, 0, 0, 512, true, true))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(2, 1, 0, 512, true, true))
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(2, 2, 0, 512, true, true))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(2, 3, 0, 512, true, true))
    assertResult(Set(CENTER, S, W, SW)) (TileBuffers.bufferRegions(2, 0, 1, 512, true, true))
    assertResult(Set(CENTER, E, SE, S)) (TileBuffers.bufferRegions(2, 1, 1, 512, true, true))
    assertResult(Set(CENTER, S, SW, W)) (TileBuffers.bufferRegions(2, 2, 1, 512, true, true))
    assertResult(Set(CENTER, S, SE, E)) (TileBuffers.bufferRegions(2, 3, 1, 512, true, true))
    assertResult(Set(CENTER, N, W, NW)) (TileBuffers.bufferRegions(2, 0, 2, 512, true, true))
    assertResult(Set(CENTER, N, NE, E)) (TileBuffers.bufferRegions(2, 1, 2, 512, true, true))
    assertResult(Set(CENTER, N, W, NW)) (TileBuffers.bufferRegions(2, 2, 2, 512, true, true))
    assertResult(Set(CENTER, N, E, NE)) (TileBuffers.bufferRegions(2, 3, 2, 512, true, true))
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(2, 0, 3, 512, true, true))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(2, 1, 3, 512, true, true))
    assertResult(Set(CENTER, W)) (TileBuffers.bufferRegions(2, 2, 3, 512, true, true))
    assertResult(Set(CENTER, E)) (TileBuffers.bufferRegions(2, 3, 3, 512, true, true))
  }

  /**
    * Establish 2 perimeters of 8 points, one inside the buffer limits and the other further inside the tile, so that
    * they should not be considered for buffering in the adjacent tiles.
    * The tile is 512px, the buffer of 32 pixels.
    * Then test all regions return the points only in the buffer.
    */
  test("Verify regions work") {
    val tile = new DensityTile
    val pixels = MMap.empty[EncodedPixelYear,Int]

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
    pixels += (encode(32,32,1970) -> 1)
    pixels += (encode(256,32,1970) -> 1)
    pixels += (encode(479,32,1970) -> 1)
    pixels += (encode(479,256,1970) -> 1)
    pixels += (encode(479,479,1970) -> 1)
    pixels += (encode(256,479,1970) -> 1)
    pixels += (encode(32,479,1970) -> 1)
    pixels += (encode(32,256,1970) -> 1)

    tile.collect(BasisOfRecord.OBSERVATION, pixels)

    // North
    assertContains(
      tile.getBufferRegion(TileBuffers.Region.N, 512, 32),
      3,
      BasisOfRecord.OBSERVATION,
      encode(16,528,1970),
      encode(256,528,1970),
      encode(496,528,1970)
    )

    // North East
    assertContains(
      tile.getBufferRegion(TileBuffers.Region.NE, 512, 32),
      1,
      BasisOfRecord.OBSERVATION,
      encode(-16,528,1970)
    )

    // East
    assertContains(
      tile.getBufferRegion(TileBuffers.Region.E, 512, 32),
      3,
      BasisOfRecord.OBSERVATION,
      encode(-16,16,1970),
      encode(-16,256,1970),
      encode(-16,496,1970)
    )

    // South East
    assertContains(
      tile.getBufferRegion(TileBuffers.Region.SE, 512, 32),
      1,
      BasisOfRecord.OBSERVATION,
      encode(-16,-16,1970)
    )

    // South
    assertContains(
      tile.getBufferRegion(TileBuffers.Region.S, 512, 32),
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
