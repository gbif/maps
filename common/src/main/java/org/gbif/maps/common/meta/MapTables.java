package org.gbif.maps.common.meta;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The tables used for mapping and when they were generated.
 */
public class MapTables implements Serializable {
  private static final Pattern PIPE = Pattern.compile("\\|");
  private final String tileTable;
  private final String pointTable;

  public MapTables(String tileTable, String pointTable) {
    this.tileTable = tileTable;
    this.pointTable = pointTable;
  }

  public String getPointTable() {
    return pointTable;
  }

  public String getTileTable() {
    return tileTable;
  }

  @Override
  public String toString() {
    return "MapTables{" +
           "tileTable=" + tileTable +
           ", pointTable=" + pointTable +
           '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MapTables mapTables = (MapTables) o;
    return Objects.equals(tileTable, mapTables.tileTable) &&
           Objects.equals(pointTable, mapTables.pointTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tileTable, pointTable);
  }

  /**
   * @return a human readable string (as bytes) for serialization
   */
  public byte[] serialize() {
    return (tileTable + "|" + pointTable).getBytes();
  }

  /**
   * Builder for deserializing from the byte array.
   */
  public static MapTables deserialize(byte[] encoded) {
    if (encoded == null || encoded.length ==0) {
      throw new IllegalArgumentException("Unable to decode into MapTables - no data supplied");
    }
    String s = new String(encoded);
    String[] fields = PIPE.split(s);
    if(fields.length == 2) {
      return new MapTables(fields[0], fields[1]);
    }
    throw new IllegalArgumentException("Unable to decode into MapTables:" + new String(encoded));
  }
}
