package org.gbif.maps.common.projection;

public class Mercator {
  private final int tileSize;

  public Mercator(int tileSize) {
    if (tileSize == 256 || tileSize == 512 || tileSize == 1024 || tileSize == 2048 || tileSize == 4096) {
      this.tileSize = tileSize;
    } else {
      throw new RuntimeException("Cannot create Mercator with supplied tileSize[" + tileSize + "]. "
                                 + "Only the following tile sizes are supported: 256, 512, 1024, 2048, 4096");
    }
  }

  /**
   * The circumference of the earth at the equator in meter.
   */
  private static final double EARTH_CIRCUMFERENCE = 40075016.686;

  /**
   * Calculates the distance on the ground that is represented by a single pixel on the map.
   *
   * @param latitude the latitude coordinate at which the resolution should be calculated.
   * @param zoom     the zoom level at which the resolution should be calculated.
   *
   * @return the ground resolution at the given latitude and zoom level.
   */
  public double calculateGroundResolution(double latitude, byte zoom) {
    return Math.cos(latitude * (Math.PI / 180)) * EARTH_CIRCUMFERENCE
           / ((long) tileSize << zoom);
  }

  /**
   * Converts a latitude coordinate (in degrees) to a pixel Y coordinate at a certain zoom level.
   *
   * @param latitude the latitude coordinate that should be converted.
   * @param zoom     the zoom level at which the coordinate should be converted.
   *
   * @return the pixel Y coordinate of the latitude value.
   */
  public double latitudeToPixelY(double latitude, byte zoom) {
    double sinLatitude = Math.sin(latitude * (Math.PI / 180));
    return (0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI))
           * ((long) tileSize << zoom);
  }

  /**
   * Returns the pixel location relative to the top left of the tile.
   * @param latitude To convert
   * @param zoom  the zoom level at which the coordinate should be converted.
   * @return the pixel Y of the coordinate relative to the tile
   */
  public int latitudeToTileLocalPixelY(double latitude, byte zoom) {
    double pixelY = latitudeToPixelY(latitude, zoom); // global space
    return (int) Math.floor(pixelY%tileSize);
  }

  /**
   * Returns the pixel location relative to the top left of the tile.
   * @param longitude To convert
   * @param zoom  the zoom level at which the coordinate should be converted.
   * @return the pixel Y of the coordinate relative to the tile
   */
  public int longitudeToTileLocalPixelX(double longitude, byte zoom) {
    double pixelX = longitudeToPixelX(longitude, zoom); // global space
    return (int) Math.floor(pixelX%tileSize);
  }

  /**
   * Converts a latitude coordinate (in degrees) to a tile Y number at a certain zoom level.
   *
   * @param latitude the latitude coordinate that should be converted.
   * @param zoom     the zoom level at which the coordinate should be converted.
   *
   * @return the tile Y number of the latitude value.
   */
  public long latitudeToTileY(double latitude, byte zoom) {
    return pixelYToTileY(latitudeToPixelY(latitude, zoom), zoom);
  }

  /**
   * Converts a longitude coordinate (in degrees) to a pixel X coordinate at a certain zoom level.
   *
   * @param longitude the longitude coordinate that should be converted.
   * @param zoom      the zoom level at which the coordinate should be converted.
   *
   * @return the pixel X coordinate of the longitude value.
   */
  public double longitudeToPixelX(double longitude, byte zoom) {
    return (longitude + 180) / 360 * ((long) tileSize << zoom);
  }

  /**
   * Converts a longitude coordinate (in degrees) to the tile X number at a certain zoom level.
   *
   * @param longitude the longitude coordinate that should be converted.
   * @param zoom      the zoom level at which the coordinate should be converted.
   *
   * @return the tile X number of the longitude value.
   */
  public long longitudeToTileX(double longitude, byte zoom) {
    return pixelXToTileX(longitudeToPixelX(longitude, zoom), zoom);
  }

  /**
   * Converts a pixel X coordinate at a certain zoom level to a longitude coordinate.
   *
   * @param pixelX the pixel X coordinate that should be converted.
   * @param zoom   the zoom level at which the coordinate should be converted.
   *
   * @return the longitude value of the pixel X coordinate.
   */
  public double pixelXToLongitude(double pixelX, byte zoom) {
    return 360 * ((pixelX / ((long) tileSize << zoom)) - 0.5);
  }

  /**
   * Converts a pixel X coordinate to the tile X number.
   *
   * @param pixelX the pixel X coordinate that should be converted.
   * @param zoom   the zoom level at which the coordinate should be converted.
   *
   * @return the tile X number.
   */
  public long pixelXToTileX(double pixelX, byte zoom) {
    return (long) Math.min(Math.max(pixelX / tileSize, 0), Math.pow(2, zoom) - 1);
  }

  /**
   * Converts a pixel Y coordinate at a certain zoom level to a latitude coordinate.
   *
   * @param pixelY the pixel Y coordinate that should be converted.
   * @param zoom   the zoom level at which the coordinate should be converted.
   *
   * @return the latitude value of the pixel Y coordinate.
   */
  public double pixelYToLatitude(double pixelY, byte zoom) {
    double y = 0.5 - (pixelY / ((long) tileSize << zoom));
    return 90 - 360 * Math.atan(Math.exp(-y * (2 * Math.PI))) / Math.PI;
  }

  /**
   * Converts a pixel Y coordinate to the tile Y number.
   *
   * @param pixelY the pixel Y coordinate that should be converted.
   * @param zoom   the zoom level at which the coordinate should be converted.
   *
   * @return the tile Y number.
   */
  public long pixelYToTileY(double pixelY, byte zoom) {
    return (long) Math.min(Math.max(pixelY / tileSize, 0), Math.pow(2, zoom) - 1);
  }

  /**
   * Converts a tile X number at a certain zoom level to a longitude coordinate.
   *
   * @param tileX the tile X number that should be converted.
   * @param zoom  the zoom level at which the number should be converted.
   *
   * @return the longitude value of the tile X number.
   */
  public double tileXToLongitude(long tileX, byte zoom) {
    return pixelXToLongitude(tileX * tileSize, zoom);
  }

  /**
   * Converts a tile Y number at a certain zoom level to a latitude coordinate.
   *
   * @param tileY the tile Y number that should be converted.
   * @param zoom  the zoom level at which the number should be converted.
   *
   * @return the latitude value of the tile Y number.
   */
  public double tileYToLatitude(long tileY, byte zoom) {
    return pixelYToLatitude(tileY * tileSize, zoom);
  }
}
