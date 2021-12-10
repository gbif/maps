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

    assertEquals("Date has not been handled correctly", "2017-07-10T12:34Z", m1.getTileTableDate());
    assertEquals("Date has not been handled correctly", "2017-02-10T12:34Z", m1.getPointTableDate());
  }

}
