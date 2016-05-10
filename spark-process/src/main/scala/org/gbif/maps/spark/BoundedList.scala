package org.gbif.maps.spark

import scala.collection.mutable.MutableList;

/**
  * A mutable list that is bounded by the maximum size provided at construction time.
  * Once the size is reached, any additions do nothing.
  */
class BoundedList[A](maxSize: Int)  extends MutableList[A] {

  override def prependElem(elem: A) = {
    if (len < maxSize) super.prependElem(elem);
  }

  override def appendElem(elem: A) = {
    if (len < maxSize) super.appendElem(elem);
  }
}
