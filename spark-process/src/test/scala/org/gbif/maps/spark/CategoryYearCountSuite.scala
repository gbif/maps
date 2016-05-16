package org.gbif.maps.spark

import org.scalatest.FunSuite

class CategoryYearCountSuite extends FunSuite {

  test("Categories should be created") {

    val d = new CategoryYearCount[String]()
      .collect("C1", 2001, 1)
      .collect("C2", 2001, 1)
      .collect("C2", 2001, 1)
      .collect("C3", 2001, 1)
      .build()

    assert(d.size === 3)
    assert(d.contains("C1"))
    assert(d.contains("C2"))
    assert(d.contains("C3"))
  }

  test("Years should be created and increment") {
    val d = new CategoryYearCount[String]()
      .collect("C1", 2001, 1)
      .collect("C1", 2001, 2)
      .collect("C1", 2002, 1)
      .collect("C2", 2002, 1)
      .build()

    assert(d.size === 2)
    assert(d.contains("C1"))
    assert(d.contains("C2"))
    assert(d("C1").size === 2)
    assert(d("C1")(2001) === 3)
    assert(d("C1")(2002) === 1)
    assert(d("C2")(2002) === 1)
  }

  test("Merging should fold correctly") {
    val c1 = new CategoryYearCount[String]()
      .collect("C1", 2001, 1)
      .collect("C1", 2002, 1)

    val c2 = new CategoryYearCount[String]()
      .collect("C1", 2001, 10)
      .collect("C1", 2002, 20)
      .collect("C2", 2001, 1)

    val d = c1.merge(c2).build()

    assert(d.size === 2)
    assert(d.contains("C1"))
    assert(d.contains("C2"))
    assert(d("C1").size === 2)
    assert(d("C1")(2001) === 11)
    assert(d("C1")(2002) === 21)
    assert(d("C2")(2001) === 1)
  }
}
