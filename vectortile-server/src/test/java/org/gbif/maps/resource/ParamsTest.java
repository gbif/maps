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
package org.gbif.maps.resource;

import org.junit.Test;

import static org.gbif.maps.resource.Params.MAX_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.MAX_SQUARE_SIZE;
import static org.gbif.maps.resource.Params.validateBinning;

public class ParamsTest {

  @Test
  public void testValidBinning() {
    validateBinning(null, Integer.MAX_VALUE, 0); // sizes are ignored when not binning
    validateBinning("hex", 1, 0);
    validateBinning("HEX", MAX_HEX_PER_TILE, 0);
    validateBinning("square", 0, 1);
    validateBinning("square", 0, MAX_SQUARE_SIZE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHexPerTileTooLarge() {
    validateBinning("hex", MAX_HEX_PER_TILE + 1, 16);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHexPerTileZero() {
    validateBinning("hex", 0, 16);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSquareSizeZero() {
    validateBinning("square", 51, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSquareSizeTooLarge() {
    validateBinning("square", 51, MAX_SQUARE_SIZE + 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownBinMode() {
    validateBinning("triangle", 51, 16);
  }
}
