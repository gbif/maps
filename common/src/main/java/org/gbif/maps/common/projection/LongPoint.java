package org.gbif.maps.common.projection;

import java.io.Serializable;

/**
 * A container of X,Y which are long .
 */
public class LongPoint {
  private final long x;
  private final long y;

  public LongPoint(long x, long y) {
    this.x = x;
    this.y = y;
  }

  public long getX() {
    return x;
  }

  public long getY() {
    return y;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LongPoint longPoint = (LongPoint) o;

    if (x != longPoint.x) return false;
    if (y != longPoint.y) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (x ^ (x >>> 32));
    result = 31 * result + (int) (y ^ (y >>> 32));
    return result;
  }
}
