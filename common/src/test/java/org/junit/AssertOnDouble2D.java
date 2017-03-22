package org.junit;

import org.gbif.maps.common.projection.Double2D;

public class AssertOnDouble2D extends org.junit.Assert {

  private static boolean doubleIsDifferent(double d1, double d2, double delta) {
    return Double.compare(d1, d2) == 0?false:Math.abs(d1 - d2) > delta;
  }

  public static void assertEquals(String message, Double2D expected, Double2D actual, double delta) {
    if (doubleIsDifferent(expected.getX(), actual.getX(), delta) || doubleIsDifferent(expected.getY(), actual.getY(), delta)) {
      failNotEquals(message, expected, actual);
    }
  }

  public static void assertEquals(Double2D expected, Double2D actual, double delta) {
    assertEquals("", expected, actual, delta);
  }

  private static void failNotEquals(String message, Object expected, Object actual) {
    fail(format(message, expected, actual));
  }
}
