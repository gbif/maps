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
package org.gbif.maps.common.bin;

import org.codetome.hexameter.core.api.HexagonOrientation;
import org.codetome.hexameter.core.api.HexagonalGrid;
import org.codetome.hexameter.core.api.HexagonalGridBuilder;
import org.codetome.hexameter.core.api.HexagonalGridLayout;
import org.junit.Test;

import static org.junit.Assert.*;

public class HexBinTest {

  @Test
  public void testNewGridInstance() {
    HexagonalGrid grid = new HexagonalGridBuilder()
      .setGridWidth(3)
      .setGridHeight(3)
      .setGridLayout(HexagonalGridLayout.RECTANGULAR)
      .setOrientation(HexagonOrientation.FLAT_TOP)
      .setRadius(300)
      .build();

    System.out.println(grid.getByPixelCoordinate(1650,0).isPresent());  // true

    // I expected all these with negative x to return false
    // obviously I don't understand where 0,0 is referenced from
    System.out.println(grid.getByPixelCoordinate(-100,0).isPresent());  // true
    System.out.println(grid.getByPixelCoordinate(-200,0).isPresent());  // true
    System.out.println(grid.getByPixelCoordinate(-500,0).isPresent());  // false
    System.out.println(grid.getByPixelCoordinate(-600,0).isPresent());  // false
    System.out.println(grid.getByPixelCoordinate(-1000,0).isPresent()); // false

  }

}
