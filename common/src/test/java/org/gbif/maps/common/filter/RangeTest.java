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
