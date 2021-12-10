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
package org.gbif.maps.common.projection;

import java.io.Serializable;
import java.util.Objects;

/**
 * An immutable replace of {@link java.awt.Point} to contain X and Y as ints and named in a way that will play
 * nicely with all the other Point classes.
 */
public class Int2D implements Serializable {
  private static final long serialVersionUID = 3304180038973328103L;

  private final int x;
  private final int y;

  public Int2D(int x, int y) {
    this.x = x;
    this.y = y;
  }

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Int2D int2D = (Int2D) o;
    return x == int2D.x &&
           y == int2D.y;
  }

  @Override
  public int hashCode() {
    return Objects.hash(x, y);
  }

  @Override
  public String toString() {
    return "Int2D{" +
           "x=" + x +
           ", y=" + y +
           '}';
  }
}
