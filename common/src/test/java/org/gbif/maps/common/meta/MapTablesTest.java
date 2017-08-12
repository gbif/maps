package org.gbif.maps.common.meta;

import org.junit.Test;

import static org.junit.Assert.*;

public class MapTablesTest {

  /**
   * Simple positive only test to confirm normal operation.
   */
  @Test
  public void testSerde() {
    MapTables m1 = new MapTables("prod_a_tiles_20170710_1234", "prod_a_points_20170710_1234");
    MapTables m2 = MapTables.deserialize(m1.serialize());
    assertEquals("Tables are not the same after SerDe", m1, m2);
  }

}
