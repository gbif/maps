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

public abstract class AbstractTileProjection implements TileProjection {

  /*
   * Earth's authalic ("equal area") radius is the radius of a hypothetical perfect sphere that has the same surface
   * area as the reference ellipsoid.
   * @see https://en.wikipedia.org/wiki/Earth_radius#Authalic_radius
   */
  public static final double EARTH_RADIUS = 6_371_007.2;

  private final int tileSize;

  public AbstractTileProjection(int tileSize) {
    if (tileSize == 256 || tileSize == 512 || tileSize == 1024 || tileSize == 2048 || tileSize == 4096) {
      this.tileSize = tileSize;
    } else {
      throw new IllegalArgumentException("Only the following tile sizes are supported: 256, 512, 1024, 2048, 4096.  "
                                         + "Supplied: " + tileSize);
    }
  }

  public int getTileSize(){
    return tileSize;
  }
}
