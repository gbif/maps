package org.gbif.maps.common.projection;

public abstract class AbstractTileProjection implements TileProjection {

  /**
   * The circumference of the earth at the equator in meters, as used by leaflet etc.
   */
  public static final double EARTH_CIRCUMFERENCE = 40075016.686;

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
