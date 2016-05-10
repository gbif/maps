package org.gbif.maps.spark

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class Tiles[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Serializable {


}

// Builder from a PairRDD
object Tiles {
  implicit def fromPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): Tiles[K, V] =
    new Tiles[K, V](rdd)
}
