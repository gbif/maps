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
package org.gbif.maps.common.meta;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import lombok.Builder;
import lombok.Data;

/**
 * The tables used for mapping and when they were generated.
 * This splits tables into "points", "tiles" (excludes taxonomy) and a set of "tile_tables" keyed on each taxonomy.
 */
@Data
public class MapTables implements Serializable {
  private static final Pattern PIPE = Pattern.compile("\\|");
  private static final Pattern TABLE_TIMESTAMP = Pattern.compile("(20\\d\\d\\d\\d\\d\\d_\\d\\d\\d\\d)$");
  private final String pointTable;
  private final String tileTable;
  private final TreeMap<String, String> checklistTileTables; // checklistKey, table
  private final String pointTableDate;
  private final String tileTableDate;

  private final Map<String, String> checklistTileTableDates;

  public static MapTables newEmpty() {
    return new MapTables(null, null, null);
  }
  @Builder
  public MapTables(String pointTable, String tileTable, Map<String, String> checklistTileTables) {
    this.pointTable = pointTable;
    this.tileTable = tileTable;

    this.checklistTileTables = checklistTileTables == null ? new TreeMap<>() : new TreeMap<>(checklistTileTables); // consistent sorting
    this.pointTableDate = tableDate(pointTable);
    this.tileTableDate = tableDate(tileTable);

    checklistTileTableDates = new HashMap<>();
    if (checklistTileTables != null) {
      for (Map.Entry<String, String> e : checklistTileTables.entrySet()) {
        checklistTileTableDates.put(e.getKey(), tableDate(e.getValue()));
      }
    }
  }

  public String getTileTable(String checklistKey) {
    return checklistTileTables.get(checklistKey);
  }

  public MapTables copyWithNewPoint(String t) {
    return new MapTablesBuilder().pointTable(t).tileTable(tileTable).checklistTileTables(checklistTileTables).build();
  }

  public MapTables copyWithNewTile(String t) {
    return new MapTablesBuilder().pointTable(pointTable).tileTable(t).checklistTileTables(checklistTileTables).build();
  }
  public MapTables copyWithNewChecklistTable(String key, String t) {
    Map<String, String> copy = new HashMap<>(checklistTileTables);
    copy.put(key, t);
    return new MapTablesBuilder().pointTable(pointTable).tileTable(tileTable).checklistTileTables(copy).build();
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

  public byte[] serialize() {
    return encode().getBytes(StandardCharsets.UTF_8);
  }

  @VisibleForTesting
  String encode() {
    StringBuilder sb = new StringBuilder();

    sb.append(nullSafe(pointTable)).append("|")
      .append(nullSafe(tileTable));

    if (checklistTileTables != null) {
      for (Map.Entry<String, String> entry : checklistTileTables.entrySet()) {
        sb.append("|")
          .append(nullSafe(entry.getKey()))
          .append(",")
          .append(nullSafe(entry.getValue()));
      }
    }

    return sb.toString();
  }

  public static MapTables deserialize(byte[] encoded) {
    if (encoded == null || encoded.length == 0) {
      // expected e.g. on the first run where no ZK entry exists
      return new MapTables.MapTablesBuilder().build();
    }

    String decoded = new String(encoded, StandardCharsets.UTF_8);
    String[] parts = decoded.split("\\|", -1);

    if (parts.length < 2) {
      throw new IllegalArgumentException("Invalid encoded MapTables format");
    }

    String point = emptyToNull(parts[0]);
    String tile = emptyToNull(parts[1]);

    Map<String, String> checklist = new LinkedHashMap<>();

    for (int i = 2; i < parts.length; i++) {
      if (!parts[i].isEmpty()) {
        String[] kv = parts[i].split(",", 2);
        if (kv.length != 2) {
          throw new IllegalArgumentException("Invalid key,value pair: " + parts[i]);
        }
        checklist.put(emptyToNull(kv[0]), emptyToNull(kv[1]));
      }
    }

    return new MapTables(point, tile, checklist);
  }

  private static String nullSafe(String value) {
    return value == null ? "" : value;
  }

  private static String emptyToNull(String value) {
    return value.isEmpty() ? null : value;
  }
}
