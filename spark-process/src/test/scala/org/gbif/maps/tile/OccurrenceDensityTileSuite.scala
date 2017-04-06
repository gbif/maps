package org.gbif.maps.tile

import org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord
import org.scalatest.FunSuite

import scala.collection.mutable.{Map => MMap}

/**
  * Unit tests for OccurrenceDensityTile.
  */
class OccurrenceDensityTileSuite extends FunSuite {

  test("Verify accumulation of features") {
    val tile = new OccurrenceDensityTile(new ZXY(0,0,0), 512, 64)
    val pixels = MMap.empty[EncodedPixelYear,Int]

    pixels += (encode(10,10,1970) -> 10)
    pixels += (encode(10,10,1971) -> 20)
    pixels += (encode(10,11,1970) -> 30)
    tile.collect(BasisOfRecord.OBSERVATION, pixels)
    tile.collect(BasisOfRecord.HUMAN_OBSERVATION, pixels)
    tile.collect(BasisOfRecord.HUMAN_OBSERVATION, pixels) // collected twice

    assert(tile.getData().size === 2)
    val obs = tile.getData().get(BasisOfRecord.OBSERVATION).get.asInstanceOf[EncodedPixelYearCount]

    obs.data.size === 3
    assert(obs.data.size === 3)

    assert(obs.data.get(encode(10,10,1970)) === Some(10))
    assert(obs.data.get(encode(10,10,1971)) === Some(20))
    assert(obs.data.get(encode(10,11,1970)) === Some(30))

    val humanObs = tile.getData().get(BasisOfRecord.HUMAN_OBSERVATION).get.asInstanceOf[EncodedPixelYearCount]
    assert(humanObs.data.size === 3)
    assert(humanObs.data.get(encode(10,10,1970)) === Some(20))
    assert(humanObs.data.get(encode(10,10,1971)) === Some(40))
    assert(humanObs.data.get(encode(10,11,1970)) === Some(60))
  }


  test("Verify merging of separate density tiles") {
    val tile1 = new OccurrenceDensityTile(new ZXY(0,0,0), 512, 64)
    val pixels1 = MMap.empty[EncodedPixelYear,Int]
    pixels1 += (encode(10,10,1970) -> 10)
    tile1.collect(BasisOfRecord.OBSERVATION, pixels1)

    val tile2 = new OccurrenceDensityTile(new ZXY(0,0,0), 512, 64)
    val pixels2 = MMap.empty[EncodedPixelYear,Int]
    pixels2 += (encode(10,10,1970) -> 10) // same as tile1
    pixels2 += (encode(10,10,1971) -> 50)
    pixels2 += (encode(20,20,1971) -> 100)
    tile2.collect(BasisOfRecord.OBSERVATION, pixels2)

    val merged = OccurrenceDensityTile.merge(tile1, tile2)

    assert(merged.getData().size === 1) // 1 layer
    val obs = merged.getData().get(BasisOfRecord.OBSERVATION).get.asInstanceOf[EncodedPixelYearCount]
    assert(obs.data.size === 3)
    assert(obs.data.get(encode(10,10,1970)) === Some(20))
    assert(obs.data.get(encode(10,10,1971)) === Some(50))
    assert(obs.data.get(encode(20,20,1971)) === Some(100))
  }

   /**
    * Utility to check that the provided tile contains the correct number of features, and the encoded feature keys are
    * present in the basisOfRecord layer.
    */
  private def assertContains(densityTile: OccurrenceDensityTile, totalFeatures: Int, basisOfRecord: BasisOfRecord, features: EncodedPixelYear*): Unit = {
    assert(densityTile.getData().get(basisOfRecord).get.featureCount === totalFeatures)
    // ensure the map contains all the expected features
    features.foreach(f => {
      val encodedData = densityTile.getData().get(basisOfRecord).get.asInstanceOf[EncodedPixelYearCount]
      assert(encodedData.data.contains(f))
    })
  }

  // encode a pixel year easily to make tests more readable
  private def encode(x: Int, y: Int, year: Int) : EncodedPixelYear = {
    encodePixelYear(
      encodePixel(Pixel(x.asInstanceOf[Short],y.asInstanceOf[Short])),
      year.asInstanceOf[Short])
  }
}
