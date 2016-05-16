package org.gbif.maps.spark

import collection.mutable.{Map=>MMap}
import collection.immutable.{Map=>IMap}

/**
  * A container that allows for map tiles for a category (e.g. BasisOfRecord) and contains a count by year within
  * the category.
  * @tparam T The type of the category
  */
class CategoryYearCount[T] extends Serializable {
  private val data = MMap[T, MMap[Int,Int]]()

  /**
    * Collects the year and count into the category
    * @param category To collect into, creating if required
    * @param year To accumulate for
    * @param count To increment(!) for the year
    * @return this instance
    */
  def collect(category: T, year: Int, count: Int) : CategoryYearCount[T] = {
    // accumulate the count at the year, creating the category if required
    val years = data.getOrElseUpdate(category, MMap[Int,Int]())
    years(year) = years.getOrElse(year, 0) + count
    this
  }

  /**
    * Merges C into this instance by calling collect for each entry
    * @param c To copy from into ourself
    * @return this instance
    */
  def merge(c: CategoryYearCount[T]) : CategoryYearCount[T] = {
    c.build().foreach(e1 => {
      e1._2.foreach(e2 => {
        collect(e1._1, e2._1, e2._2)
      })
    })
    this
  }

  /**
    * @return An immutable copy of the data
    */
  def build() : IMap[T, IMap[Int,Int]] = {
    data.map(kv => (kv._1,kv._2.toMap)).toMap
  }
}
