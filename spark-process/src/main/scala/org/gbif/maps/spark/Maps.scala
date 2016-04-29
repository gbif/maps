package org.gbif.maps.spark

import scala.collection.mutable.Map;

/**
  * Utilities for dealing with maps.
  * This could surely be more generic, but that is beyond the current skill of this author.
  */
object Maps {
  /**
    * Merges map1 and map2 into a new immutable(!) map.
    * <ol>
    *   <li>If map2 holds a key not in map1 the entry is added to map1</li>
    *   <li>If map2 holds a key that is in map1 then the inner map is merged, accumulating any entries keyed in both maps.
    * </ol>
    */
  def merge(map1 : Map[(Int,Int), Map[Int,Int]], map2 : Map[(Int,Int), Map[Int,Int]]) = (map1.keySet ++ map2.keySet)
    .map(key => key -> mergeValues(map1.get(key), map2.get(key)))
    .toMap

  // merges the values if required, or selecting the only one if only one side has value
  private def mergeValues(o1 : Option[Map[Int,Int]], o2 : Option[Map[Int,Int]]) = (o1, o2) match {
    case (Some(v1 : Map[Int,Int]), Some(v2 : Map[Int,Int])) => Map() ++ merge2(v1,v2) // return mutable(!)
    case _ => (o1 orElse o2).get
  }

  // the inner map merge (same as the outer)
  private def merge2(map1 : Map[Int,Int], map2 : Map[Int,Int]) = (map1.keySet ++ map2.keySet)
    .map(key => key -> mergeValues2(map1.get(key), map2.get(key)))
    .toMap

  private def mergeValues2(o1 : Option[Int], o2 : Option[Int]) = (o1, o2) match {
    case (Some(v1: Int), Some(v2: Int)) => v1 + v2 // accumulate if required
    case _ => (o1 orElse o2).get
  }
}
