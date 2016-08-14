package org.gbif.maps.common.filter;

/**
 * Encapsulates a range of Integers which can be unbounded, enclosed or bounded on one side.
 * Guava and Apache commons Range implementations were considered before writing this, but they would require
 * too many if / else statements to handle the states of the range bounds (e.g. if it is bounded only on 1 side).
 */
public class Range {
  private final Integer lower;
  private final Integer upper;

  /**
   * Constructs the range, which can be unbounded on either sides by supplying null.
   * @param lower the lower bounds where null indicates unbounded
   * @param upper the upper bounds where null indicates unbounded
   */
  public Range(Integer lower, Integer upper) {
    if (lower!=null & upper!=null && lower>upper) {
      throw new IllegalArgumentException("Lower cannot be a greater value than upper in a Range.");
    }
    this.lower = lower;
    this.upper = upper;
  }

  /**
   * If the range is unbounded on both sides, then returns true.  Otherwise ensures that the year given is not null and
   * is contained in the the range.  Containment is inclusive on both sides - i.e. if the value is equal to the bound
   * on either side, then it returns true.
   *
   * @param value to test
   * @return true if the range is unbounded or if the value is within the range (inclusive on boundaries)
   */
  public boolean isContained(Integer value) {
    return (lower == null || (value != null && value >= lower)) &&
           (upper == null || (value != null && value <= upper));
  }

  /**
   * @return true of there are no bounds contained.
   */
  public boolean isUnbounded() {
    return (lower == null  && upper == null);
  }

}
