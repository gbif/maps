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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import static org.junit.Assert.*;

public class MapTablesTest {

  @Test
  public void testSerde1() {
    MapTables m1 = new MapTables("prod_a_points_20170210_1234", "prod_a_tiles_20170710_1234", new HashMap<>());
    MapTables m2 = MapTables.deserialize(m1.serialize());
    assertEquals("Tables are not the same after SerDe", m1, m2);
    assertEquals("Date has not been handled correctly", "2017-02-10T12:34Z", m1.getPointTableDate());
    assertEquals("Date has not been handled correctly", "2017-07-10T12:34Z", m1.getTileTableDate());
  }

  @Test
  public void testSerde2() {
    Map<String, String> checklist = new HashMap<>() {{
      put("B", "prod_a_tiles_B_20260214_1235");
      put("A", "prod_a_tiles_A_20260213_1234");
    }};
    MapTables m1 = new MapTables("prod_a_points_20170210_1234", "prod_a_tiles_20170710_1234", checklist);
    MapTables m2 = MapTables.deserialize(m1.serialize());
    assertEquals("Tables are not the same after SerDe", m1, m2);
    assertEquals("Date has not been handled correctly", "2017-02-10T12:34Z", m1.getPointTableDate());
    assertEquals("Date has not been handled correctly", "2017-07-10T12:34Z", m1.getTileTableDate());
    assertEquals("Date has not been handled correctly", "2026-02-13T12:34Z", m1.getChecklistTileTableDates().get("A"));
    assertEquals("Date has not been handled correctly", "2026-02-14T12:35Z", m1.getChecklistTileTableDates().get("B"));
  }

  @Test
  public void testEncode() {
    Map<String, String> checklist = new HashMap<>() {{
      put("B", "tiles_B_20260214_1235");
      put("A", "tiles_A_20260213_1234");
    }};
    MapTables m1 = new MapTables("points_20170210_1234", "tiles_20170710_1234", checklist);
    assertEquals("Encoding is not as expected", "points_20170210_1234|tiles_20170710_1234|A,tiles_A_20260213_1234|B,tiles_B_20260214_1235", m1.encode());
  }

  @Test
  public void testCopyPoints() {
    TreeMap<String, String> checklist = new TreeMap<>() {{
      put("B", "B_20260214_1235");
      put("A", "A_20260213_1234");
    }};
    MapTables m1 = new MapTables("points_20170210_1234", "tiles_20170710_1234", checklist);
    MapTables m2 = m1.copyWithNewPoint("points_20260101_1234");
    assertNotEquals("Tables are the same after copy and replace", m1, m2);
    assertEquals("Points have not changed to expected", "points_20260101_1234", m2.getPointTable());
    assertEquals("Tiles have changed", "tiles_20170710_1234", m2.getTileTable());
    assertEquals("Checklists have changed", checklist, m2.getChecklistTileTables());
  }

  @Test
  public void testCopyTiles() {
    TreeMap<String, String> checklist = new TreeMap<>() {{
      put("B", "B_20260214_1235");
      put("A", "A_20260213_1234");
    }};
    MapTables m1 = new MapTables("points_20170210_1234", "tiles_20170710_1234", checklist);
    MapTables m2 = m1.copyWithNewTile("tiles_20260101_1234");
    assertNotEquals("Tables are the same after copy and replace", m1, m2);
    assertEquals("Points have changed", "points_20170210_1234", m2.getPointTable());
    assertEquals("Tiles have not changed to expected", "tiles_20260101_1234", m2.getTileTable());
    assertEquals("Checklists have changed", checklist, m2.getChecklistTileTables());
  }

  @Test
  public void testCopyChecklist() {
    TreeMap<String, String> checklist = new TreeMap<>() {{
      put("B", "B_20260214_1235");
      put("A", "A_20260213_1234");
    }};
    MapTables m1 = new MapTables("points_20170210_1234", "tiles_20170710_1234", checklist);
    MapTables m2 = m1.copyWithNewChecklistTable("A", "UPDATED_20270101_1234");
    assertNotEquals("Tables are the same after copy and replace", m1, m2);
    assertEquals("Points have changed", "points_20170210_1234", m2.getPointTable());
    assertEquals("Tiles have changed", "tiles_20170710_1234", m2.getTileTable());
    assertNotEquals("Checklists have not changed", checklist, m2.getChecklistTileTables());
    assertEquals("A checklist not changed", "UPDATED_20270101_1234", m2.getChecklistTileTables().get("A"));
    assertEquals("B checklist has changed", "B_20260214_1235", m2.getChecklistTileTables().get("B"));

    MapTables m3 = m2.copyWithNewChecklistTable("C", "NEW_20270101_1234");
    assertNotEquals("Tables are the same after copy and replace", m2, m3);
    assertEquals("Points have changed", "points_20170210_1234", m3.getPointTable());
    assertEquals("Tiles have changed", "tiles_20170710_1234", m3.getTileTable());
    assertNotEquals("Checklists have not changed", checklist, m3.getChecklistTileTables());
    assertEquals("A checklist has changed", "UPDATED_20270101_1234", m3.getChecklistTileTables().get("A"));
    assertEquals("B checklist has changed", "B_20260214_1235", m3.getChecklistTileTables().get("B"));
    assertEquals("C checklist not as expected", "NEW_20270101_1234", m3.getChecklistTileTables().get("C"));
  }
}
