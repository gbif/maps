package org.gbif.maps.tile

import org.gbif.maps.common.projection.TileSchema
import org.gbif.maps.tile.TileUtils.Region._
import org.scalatest.FunSuite

/**
  * Unit tests for TileUtils.
  */
class TileUtilsSuite extends FunSuite {

  // this is critical path stuff so test all combinations that will be used
  test("Verify maximum tile address") {
    assertResult(( 0, 0)) (TileUtils.maxTileAddressForZoom(TileSchema.WEB_MERCATOR, 0))
    assertResult(( 1, 1)) (TileUtils.maxTileAddressForZoom(TileSchema.WEB_MERCATOR, 1))
    assertResult(( 3, 3)) (TileUtils.maxTileAddressForZoom(TileSchema.WEB_MERCATOR, 2))
    assertResult(( 7, 7)) (TileUtils.maxTileAddressForZoom(TileSchema.WEB_MERCATOR, 3))
    assertResult((15,15)) (TileUtils.maxTileAddressForZoom(TileSchema.WEB_MERCATOR, 4))

    assertResult(( 1, 0)) (TileUtils.maxTileAddressForZoom(TileSchema.WGS84_PLATE_CAREÉ, 0))
    assertResult(( 3, 1)) (TileUtils.maxTileAddressForZoom(TileSchema.WGS84_PLATE_CAREÉ, 1))
    assertResult(( 7, 3)) (TileUtils.maxTileAddressForZoom(TileSchema.WGS84_PLATE_CAREÉ, 2))
    assertResult((15, 7)) (TileUtils.maxTileAddressForZoom(TileSchema.WGS84_PLATE_CAREÉ, 3))
    assertResult((31,15)) (TileUtils.maxTileAddressForZoom(TileSchema.WGS84_PLATE_CAREÉ, 4))
  }


  // this is critical path stuff so test all combinations that will be used
  test("Verify buffer regions") {

    // zoom 0
    assertResult(Set(TILE)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(0, 0, 0), false))
    assertResult(Set(TILE, E, W)) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(0, 0, 0), false))
    assertResult(Set(TILE, E, W)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(0, 0, 0), false))
    assertResult(Set(TILE, E, W)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(0, 1, 0), false))

    // zoom 1, no downscaling, no wrapping
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 0, 0), false))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 0, 1), false))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 1, 0), false))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 1, 1), false))

    // zoom 1, downscaling, no wrapping
    assertResult(Set(TILE)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 0, 0), true))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 0, 1), true))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 1, 0), true))
    assertResult(Set(TILE)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(1, 1, 1), true))

    // zoom 1, downscaling, dateline wrapping
    assertResult(Set(TILE,    W)) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(1, 0, 0), true))
    assertResult(Set(TILE,    W)) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(1, 0, 1), true))
    assertResult(Set(TILE, E   )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(1, 1, 0), true))
    assertResult(Set(TILE, E   )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(1, 1, 1), true))

    // zoom 1, downscaling, dateline wrapping
    //  □□■■
    //  □□■■
    assertResult(Set(TILE,    W)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 0, 0), true))
    assertResult(Set(TILE,    W)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 0, 1), true))
    assertResult(Set(TILE, E   )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 1, 0), true))
    assertResult(Set(TILE, E   )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 1, 1), true))
    assertResult(Set(TILE,    W)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 2, 0), true)) // ■
    assertResult(Set(TILE,    W)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 2, 1), true)) // ■
    assertResult(Set(TILE, E   )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 3, 0), true)) // ■
    assertResult(Set(TILE, E   )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(1, 3, 1), true)) // ■

    // zoom 2, no downscaling, no wrapping
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 0), false))
    assertResult(Set(TILE, N, NE, E, SE, S           )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 1), false))
    assertResult(Set(TILE, N, NE, E, SE, S           )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 2), false))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 3), false))
    assertResult(Set(TILE,        E, SE, S, SW, W    )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 0), false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 1), false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 2), false))
    assertResult(Set(TILE, N, NE, E,            W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 3), false))
    assertResult(Set(TILE,        E, SE, S, SW, W    )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 0), false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 1), false))
    assertResult(Set(TILE, N, NE, E, SE, S, SW, W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 2), false))
    assertResult(Set(TILE, N, NE, E,            W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 3), false))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 0), false))
    assertResult(Set(TILE, N,            S, SW, W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 1), false))
    assertResult(Set(TILE, N,            S, SW, W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 2), false))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 3), false))

    // zoom 2, downscaling, no wrapping
    assertResult(Set(TILE                            )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 0), true))
    assertResult(Set(TILE,               S           )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 1), true))
    assertResult(Set(TILE, N                         )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 2), true))
    assertResult(Set(TILE                            )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 0, 3), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 0), true))
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 1), true))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 2), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 1, 3), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 0), true))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 1), true))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 2), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 2, 3), true))
    assertResult(Set(TILE                            )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 0), true))
    assertResult(Set(TILE,               S           )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 1), true))
    assertResult(Set(TILE, N                         )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 2), true))
    assertResult(Set(TILE                            )) (TileUtils.bufferRegions(TileSchema.POLAR, new ZXY(2, 3, 3), true))

    // zoom 2, downscaling, dateline wrapping
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 0, 0), true))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 0, 1), true))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 0, 2), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 0, 3), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 1, 0), true))
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 1, 1), true))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 1, 2), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 1, 3), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 2, 0), true))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 2, 1), true))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 2, 2), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 2, 3), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 3, 0), true))
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 3, 1), true))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 3, 2), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WEB_MERCATOR, new ZXY(2, 3, 3), true))

    // zoom 2, downscaling, dateline wrapping
    //  □□□□□□□□
    //  □□□□□□□□
    //  □□■■□□□□
    //  □□■■□□□□
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 0, 0), true))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 0, 1), true))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 0, 2), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 0, 3), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 1, 0), true))
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 1, 1), true))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 1, 2), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 1, 3), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 2, 0), true))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 2, 1), true))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 2, 2), true)) // ■
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 2, 3), true)) // ■
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 3, 0), true))
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 3, 1), true))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 3, 2), true)) // ■
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 3, 3), true)) // ■
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 4, 0), true))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 4, 1), true))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 4, 2), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 4, 3), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 5, 0), true))
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 5, 1), true))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 5, 2), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 5, 3), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 6, 0), true))
    assertResult(Set(TILE,               S, SW, W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 6, 1), true))
    assertResult(Set(TILE, N,                   W, NW)) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 6, 2), true))
    assertResult(Set(TILE,                      W    )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 6, 3), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 7, 0), true))
    assertResult(Set(TILE,        E, SE, S           )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 7, 1), true))
    assertResult(Set(TILE, N, NE, E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 7, 2), true))
    assertResult(Set(TILE,        E                  )) (TileUtils.bufferRegions(TileSchema.WGS84_PLATE_CAREÉ, new ZXY(2, 7, 3), true))
  }

  // test date line handling of addressing of adjacent tiles
  test("Adjacent tile handling") {
    val zoom = 16
    val (maxAddress, _) = TileUtils.maxTileAddressForZoom(TileSchema.WEB_MERCATOR, zoom)
    assertResult(new ZXY(zoom, maxAddress, 10)) (TileUtils.adjacentTileZXY(TileSchema.WEB_MERCATOR, new ZXY(zoom,0,10), W))
  }
}
