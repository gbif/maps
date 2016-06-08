package org.gbif.maps.common.projection;

import java.io.Serializable;
import java.util.Objects;

/**
 * An immutable replace of {@link java.awt.geom.Point2D.java.awt.geom.Point2D.Double} to contain X and Y as doubles
 * and named in a way that will play nicely with all the other Point classes.
 */
public class Double2D implements Serializable {
  private static final long serialVersionUID = 4586026386756756496L;
  private final double x;
  private final double y;

  public Double2D(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Double2D double2D = (Double2D) o;
    return x == double2D.x &&
           y == double2D.y;
  }

  @Override
  public int hashCode() {
    return Objects.hash(x, y);
  }

  @Override
  public String toString() {
    return "Double2D{" +
           "x=" + x +
           ", y=" + y +
           '}';
  }
}
