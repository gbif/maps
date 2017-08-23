package org.gbif.maps.common.meta;

import org.junit.Test;

import static org.junit.Assert.*;

public class MapTablesTest {

  /**
   * Simple positive only test to confirm normal operation.
   */
  @Test
  public void testSerde() {
    MapTables m1 = new MapTables("prod_a_tiles_20170710_1234", "prod_a_points_20170210_1234");
    MapTables m2 = MapTables.deserialize(m1.serialize());
    assertEquals("Tables are not the same after SerDe", m1, m2);

    // Test assumes the client's local timezone is Europe/Copenhagen (or equivalent).
    assertEquals("Date has not been handled correctly", "2017-07-10T10:34Z", m1.getTileTableDate());
    assertEquals("Date has not been handled correctly", "2017-02-10T11:34Z", m1.getPointTableDate());
  }

}
