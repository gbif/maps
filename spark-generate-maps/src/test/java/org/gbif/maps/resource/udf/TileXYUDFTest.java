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
package org.gbif.maps.resource.udf;

import org.gbif.maps.common.projection.Long2D;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.resource.udf.TileXYUDF.Direction;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import static org.gbif.maps.resource.udf.TileXYUDF.Direction.*;
import static org.junit.Assert.assertEquals;

public class TileXYUDFTest {

  @Test
  public void adjacentTileAddress() {
    // should all behave the same
    List<String> codes = Lists.newArrayList("EPSG:3857", "EPSG:3575", "EPSG:3031");

    for (String epsg : codes) {
      TileSchema schema = TileSchema.fromSRS(epsg);
      for (Direction d : TileXYUDF.Direction.values()) {
        assertEquals(
            new Long2D(0, 0), TileXYUDF.adjacentTileAddress(schema, 0, d, new Long2D(0, 0)));
      }

      assertEquals(new Long2D(0, 1), TileXYUDF.adjacentTileAddress(schema, 1, N, new Long2D(0, 0)));
      assertEquals(new Long2D(0, 0), TileXYUDF.adjacentTileAddress(schema, 1, E, new Long2D(1, 0)));
    }
  }

  @Test
  public void adjacentTileAddress4326() {
    TileSchema schema = TileSchema.fromSRS("EPSG:4326");

    assertEquals(new Long2D(0, 0), TileXYUDF.adjacentTileAddress(schema, 0, N, new Long2D(0, 0)));
    assertEquals(new Long2D(0, 0), TileXYUDF.adjacentTileAddress(schema, 0, S, new Long2D(0, 0)));
    assertEquals(new Long2D(1, 0), TileXYUDF.adjacentTileAddress(schema, 0, N, new Long2D(1, 0)));
    assertEquals(new Long2D(1, 0), TileXYUDF.adjacentTileAddress(schema, 0, S, new Long2D(1, 0)));
    assertEquals(new Long2D(1, 0), TileXYUDF.adjacentTileAddress(schema, 0, W, new Long2D(0, 0)));
    assertEquals(new Long2D(0, 0), TileXYUDF.adjacentTileAddress(schema, 0, E, new Long2D(1, 0)));
  }
}
