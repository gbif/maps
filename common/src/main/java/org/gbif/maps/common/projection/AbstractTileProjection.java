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
