/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.junit;

import org.gbif.maps.common.projection.Double2D;

import static org.junit.Assert.fail;
import static org.junit.Assert.format;

public class AssertOnDouble2D {

  private static boolean doubleIsDifferent(double d1, double d2, double delta) {
    return Double.compare(d1, d2) != 0 && Math.abs(d1 - d2) > delta;
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
