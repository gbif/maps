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

/**
 * Describes a tile schema; that is, how the tiles are arranged over the extent of a projection.
 */
public enum TileSchema {
  WGS84_PLATE_CAREÉ (2, 1, true),
  WEB_MERCATOR (1, 1, true),
  POLAR (1, 1, false);

  private final int zzTilesHorizontal;
  private final int zzTilesVertical;
  private final boolean wrapX;

  TileSchema(int zzTilesHorizontal, int zzTilesVertical, boolean wrapX) {
    this.zzTilesHorizontal = zzTilesHorizontal;
    this.zzTilesVertical = zzTilesVertical;
    this.wrapX = wrapX;
  }

  public static TileSchema fromSRS(String srs) {
    switch (srs.toUpperCase()) {
      case "EPSG:3857": return WEB_MERCATOR;
      case "EPSG:4326": return WGS84_PLATE_CAREÉ;
      case "EPSG:3575": return POLAR;
      case "EPSG:3031": return POLAR;
      default: return null;
    }
  }

  public int getZzTilesHorizontal() {
    return zzTilesHorizontal;
  }

  public int getZzTilesVertical() {
    return zzTilesVertical;
  }

  public boolean isWrapX() {
    return wrapX;
  }

  @Override
  public String toString() {
    return String.format("%s at zoom 0 is %d×%d; %s", this.name(), this.zzTilesHorizontal, this.zzTilesVertical, this.wrapX ? "wraps x" : "no wrap");
  }
}
