package org.gbif.maps.resource;

/**
 * Storing the tile and date, as a string for simplicity.
 */
public class DatedVectorTile {
  byte[] tile;
  String date;

  DatedVectorTile(byte[] tile, String date) {
    this.tile = tile;
    this.date = date;
  }
}
