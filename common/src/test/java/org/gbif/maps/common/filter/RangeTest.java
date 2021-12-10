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
package org.gbif.maps.common.filter;

import org.junit.Test;

import static org.junit.Assert.*;

public class RangeTest {

  /**
   * Test all combinations of range inclusion.
   */
  @Test
  public void testIsContained() {
    assertTrue(new Range(null,null).isContained(null));
    assertTrue(new Range(null,null).isContained(1));
    assertTrue(new Range(null,2014).isContained(2013));
    assertTrue(new Range(null,2014).isContained(2014)); // inclusive
    assertTrue(new Range(2014,null).isContained(2014)); // inclusive
    assertTrue(new Range(2014, null).isContained(2015));
    assertTrue(new Range(2014, 2014).isContained(2014)); // inclusive

    assertFalse(new Range(null,2014).isContained(2015));
    assertFalse(new Range(2014, null).isContained(2013));
    assertFalse(new Range(2014, 2014).isContained(2013));
    assertFalse(new Range(2014, 2014).isContained(2015));
  }

  /**
   * Test invalid range.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIAE() {
    new Range(2014,2013);
  }
}
