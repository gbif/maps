package org.gbif.maps.common.meta;

import java.io.Serializable;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The tables used for mapping and when they were generated.
 */
public class MapTables implements Serializable {
  private static final Pattern PIPE = Pattern.compile("\\|");
  private static final Pattern TABLE_TIMESTAMP = Pattern.compile("(20\\d\\d\\d\\d\\d\\d_\\d\\d\\d\\d)$");
  private final String tileTable;
  private final String pointTable;
  private final String tileTableDate;
  private final String pointTableDate;

  public MapTables(String tileTable, String pointTable) {
    this.tileTable = tileTable;
    this.pointTable = pointTable;

    this.tileTableDate = tableDate(tileTable);
    this.pointTableDate = tableDate(pointTable);
  }

  /**
   * @return the date inferred from the table name or null if it cannot be found
   */
  private String tableDate(String table) {
    if (table != null) {
      Matcher matcher = TABLE_TIMESTAMP.matcher(table);
      if (matcher.find()) {
        // It's possible to be off-by-one if tiles are generated during the DST switch.
        ZonedDateTime time = ZonedDateTime.parse(matcher.group(1),
                                                 DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").withZone(ZoneId.of("UTC")));
        return time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX").withZone(ZoneId.of("UTC")));
      }
    }
    return null;
  }

  public String getPointTable() {
    return pointTable;
  }

  public String getTileTable() {
    return tileTable;
  }

  public String getPointTableDate() {
    return pointTableDate;
  }

  public String getTileTableDate() {
    return tileTableDate;
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
    if (encoded == null || encoded.length == 0) {
      throw new IllegalArgumentException("Unable to decode into MapTables - no data supplied");
    }
    String s = new String(encoded);
    String[] fields = PIPE.split(s);
    if (fields.length == 2) {
      return new MapTables(fields[0], fields[1]);
    }
    throw new IllegalArgumentException("Unable to decode into MapTables:" + new String(encoded));
  }
}
