package org.gbif.maps.spark

import com.vividsolutions.jts.geom.GeometryFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Partitioner, SparkContext}
import org.gbif.maps.common.projection.{Double2D, Tiles}
import org.gbif.maps.io.PointFeature.PointFeatures.Feature

import scala.collection.mutable.{Map => MMap}
import scala.collection.{Set, mutable}

/**
  * A builder of HFiles for a tile pyramid.
  */
object BackfillTilesBufferSlow {
  private val GEOMETRY_FACTORY = new GeometryFactory()

  /**
    * A partitioner that puts data together by the map key
    */
  class MapKeyPartitioner[K,V](partitions: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[(String, Double2D, Feature.BasisOfRecord, Short)]
      // skip ZXY, as that would mean we can't downscale
      var hc = k._1.hashCode()
      if (hc < 0) hc = -hc
      return hc%numPartitions
    }

    override def numPartitions: Int = partitions
  }

  /**
    * Build for the given projection configuration.
    *
    * @param projectionConfig Configuration of the SRS and zoom levels etc
    */
  def build(sc :SparkContext, df : DataFrame, keys: Set[String], config: MapConfiguration, projectionConfig: ProjectionConfig) = {

    // project the data onto a global addressing scheme at the highest resolution we deal with
    var projectedSource = df.flatMap(row => {
      val projection = Tiles.fromEPSG(projectionConfig.srs, projectionConfig.tileSize)

      // Structured as: (MapKey, globalPixelXY, basisOfRecord, year) -> 1
      val res = mutable.ArrayBuffer[((String, Double2D, Feature.BasisOfRecord, Short),Int)]()

      val lat = row.getDouble(row.fieldIndex("decimallatitude"))
      val lng = row.getDouble(row.fieldIndex("decimallongitude"))

      // skip completely if we can't plot it on this projection
      if (projection.isPlottable(lat, lng)) {

        val bor = MapUtils.BASIS_OF_RECORD(row.getString(row.fieldIndex("basisofrecord")))
        val year = if (row.isNullAt(row.fieldIndex("year"))) null.asInstanceOf[Short]
        else row.getInt((row.fieldIndex("year"))).asInstanceOf[Short]

        // extract the keys for the record and filter to only those that are meant to be put in a tile pyramid
        val mapKeys = MapUtils.mapKeysForRecord(row).intersect(keys)

        // skip any rows in the source data that do not require pyramiding
        if (mapKeys.size>0) {
          // locate the tile and pixel we are dealing with
          val z = projectionConfig.maxZoom.asInstanceOf[Byte] // zoom
          val globalXY = projection.toGlobalPixelXY(lat,lng,z) // global pixel address space

          mapKeys.foreach(mapKey => {
            res += (((mapKey, globalXY, bor, year), 1))
          })
        }
      }
      res
    }).partitionBy(new MapKeyPartitioner(200)).reduceByKey(_+_, 200)


    (projectionConfig.minZoom to projectionConfig.maxZoom).reverse.foreach(z => {

      // downscale if needed (simply divide the coords by 2 each time)
      if (z != projectionConfig.maxZoom) {
        projectedSource = projectedSource.map( px => {
          val globalXY = new Double2D(px._1._2.getX / 2, px._1._2.getY / 2)
          ((px._1._1, globalXY, px._1._3, px._1._4), px._2)
        }).reduceByKey(_+_, 200)
      }

      var g1 = projectedSource.map(r => {
        // Structured as: (MapKey, globalPixelXY, basisOfRecord) -> (year,count)
        ((r._1._1, r._1._2, r._1._3), (r._1._4, r._2))
      })

      println(g1.count());
    })

  }
}
