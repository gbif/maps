package org.gbif.maps.tile

import org.gbif.maps.tile.TileUtils.Region._
import org.scalatest.FunSuite



/**
  * Unit tests for TileUtils.
  */
class TileUtilsSuite extends FunSuite {

  // this is critical path stuff so test all combinations that will be used
  test("Verify maximum tile address") {
    assertResult(0)(TileUtils.maxTileAddressForZoom(0))
    assertResult(1)(TileUtils.maxTileAddressForZoom(1))
    assertResult(3)(TileUtils.maxTileAddressForZoom(2))
    assertResult(7)(TileUtils.maxTileAddressForZoom(3))
    assertResult(15)(TileUtils.maxTileAddressForZoom(4))
  }


  // this is critical path stuff so test all combinations that will be used
  test("Verify buffer regions") {

    // zoom 0
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(0, 0, 0), false, false))
    assertResult(Set(TILE, E, W)) (TileUtils.bufferRegions(new ZXY(0, 0, 0), false, true))

    // zoom 1, no downscaling, no wrapping
    assertResult(Set(TILE, E, SE, S)) (TileUtils.bufferRegions(new ZXY(1, 0, 0), false, false))
    assertResult(Set(TILE, W, SW, S)) (TileUtils.bufferRegions(new ZXY(1, 1, 0), false, false))
    assertResult(Set(TILE, N, NE, E)) (TileUtils.bufferRegions(new ZXY(1, 0, 1), false, false))
    assertResult(Set(TILE, W, NW, N)) (TileUtils.bufferRegions(new ZXY(1, 1, 1), false, false))

    // zoom 1, downscaling, no wrapping
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(1, 0, 0), true, false))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(1, 1, 0), true, false))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(1, 0, 1), true, false))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(1, 1, 1), true, false))

    // zoom 1, downscaling, dateline wrapping
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(1, 0, 0), true, true))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(1, 1, 0), true, true))
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(1, 0, 1), true, true))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(1, 1, 1), true, true))

    // zoom 2, no downscaling, no wrapping
    assertResult(Set(TILE, E, SE, S)) (TileUtils.bufferRegions(new ZXY(2, 0, 0), false, false))
    assertResult(Set(TILE, W, SW, S, SE, E)) (TileUtils.bufferRegions(new ZXY(2, 1, 0), false, false))
    assertResult(Set(TILE, W, SW, S, SE, E)) (TileUtils.bufferRegions(new ZXY(2, 2, 0), false, false))
    assertResult(Set(TILE, W, SW, S)) (TileUtils.bufferRegions(new ZXY(2, 3, 0), false, false))
    assertResult(Set(TILE, N, NE, E, SE, S)) (TileUtils.bufferRegions(new ZXY(2, 0, 1), false, false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 1, 1), false, false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 2, 1), false, false))
    assertResult(Set(TILE, N, S, SW, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 3, 1), false, false))
    assertResult(Set(TILE, N, NE, E, SE, S)) (TileUtils.bufferRegions(new ZXY(2, 0, 2), false, false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 1, 2), false, false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 2, 2), false, false))
    assertResult(Set(TILE, N, S, SW, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 3, 2), false, false))
    assertResult(Set(TILE, N, NE, E)) (TileUtils.bufferRegions(new ZXY(2, 0, 3), false, false))
    assertResult(Set(TILE, N, NE, E, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 1, 3), false, false))
    assertResult(Set(TILE, N, NE, E, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 2, 3), false, false))
    assertResult(Set(TILE, N, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 3, 3), false, false))

    // zoom 2, downscaling, no wrapping
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(2, 0, 0), true, false))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(2, 1, 0), true, false))
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(2, 2, 0), true, false))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(2, 3, 0), true, false))
    assertResult(Set(TILE, S)) (TileUtils.bufferRegions(new ZXY(2, 0, 1), true, false))
    assertResult(Set(TILE, E, SE, S)) (TileUtils.bufferRegions(new ZXY(2, 1, 1), true, false))
    assertResult(Set(TILE, S, SW, W)) (TileUtils.bufferRegions(new ZXY(2, 2, 1), true, false))
    assertResult(Set(TILE, S)) (TileUtils.bufferRegions(new ZXY(2, 3, 1), true, false))
    assertResult(Set(TILE, N)) (TileUtils.bufferRegions(new ZXY(2, 0, 2), true, false))
    assertResult(Set(TILE, N, NE, E)) (TileUtils.bufferRegions(new ZXY(2, 1, 2), true, false))
    assertResult(Set(TILE, N, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 2, 2), true, false))
    assertResult(Set(TILE, N)) (TileUtils.bufferRegions(new ZXY(2, 3, 2), true, false))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(2, 0, 3), true, false))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(2, 1, 3), true, false))
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(2, 2, 3), true, false))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(new ZXY(2, 3, 3), true, false))

    // zoom 2, downscaling, wrapping
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(2, 0, 0), true, true))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(2, 1, 0), true, true))
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(2, 2, 0), true, true))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(2, 3, 0), true, true))
    assertResult(Set(TILE, S, W, SW)) (TileUtils.bufferRegions(new ZXY(2, 0, 1), true, true))
    assertResult(Set(TILE, E, SE, S)) (TileUtils.bufferRegions(new ZXY(2, 1, 1), true, true))
    assertResult(Set(TILE, S, SW, W)) (TileUtils.bufferRegions(new ZXY(2, 2, 1), true, true))
    assertResult(Set(TILE, S, SE, E)) (TileUtils.bufferRegions(new ZXY(2, 3, 1), true, true))
    assertResult(Set(TILE, N, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 0, 2), true, true))
    assertResult(Set(TILE, N, NE, E)) (TileUtils.bufferRegions(new ZXY(2, 1, 2), true, true))
    assertResult(Set(TILE, N, W, NW)) (TileUtils.bufferRegions(new ZXY(2, 2, 2), true, true))
    assertResult(Set(TILE, N, E, NE)) (TileUtils.bufferRegions(new ZXY(2, 3, 2), true, true))
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(2, 0, 3), true, true))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(2, 1, 3), true, true))
    assertResult(Set(TILE, W)) (TileUtils.bufferRegions(new ZXY(2, 2, 3), true, true))
    assertResult(Set(TILE, E)) (TileUtils.bufferRegions(new ZXY(2, 3, 3), true, true))
  }

  // test date line handling of addressing of adjacent tiles
  test("Adjacent tile handling") {
    val zoom = 16
    val maxAddress = TileUtils.maxTileAddressForZoom(zoom)
    assertResult(new ZXY(zoom, maxAddress, 10)) (TileUtils.adjacentTileZXY(new ZXY(zoom,0,10), W))

  }
}
