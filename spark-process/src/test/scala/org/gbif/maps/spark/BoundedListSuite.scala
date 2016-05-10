package org.gbif.maps.spark

import org.scalatest.FunSuite

class BoundedListSuite extends FunSuite {

  test("A bounded list should bound") {
    val l = new BoundedList[Int](3)
    l+=(1,2,3,4)
    assert(l.size == 3)
    assert(l.get(2).get == 3)
  }

  test("A bounded list should bound when merging") {
    val l1 = new BoundedList[Int](3)
    val l2 = new BoundedList[Int](3)
    l1+=(1,2)
    l2+=(3,4)
    l1++=l2
    assert(l1.size == 3)
    assert(l1.get(2).get == 3)
  }

}
