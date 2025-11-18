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
package org.gbif.maps.common.hbase;

import java.io.Serializable;
import java.nio.charset.Charset;

import com.google.common.annotations.VisibleForTesting;

/**
 * Defines the salting behavior for encoding keys in HBase.
 * <p/>
 * For our purposes, salting is used primarily to allow a simple way to create presplit tables with equal key
 * distributions, and as a secondary concern it will likely improve the concurrent read throughput by balancing
 * requests across the region servers.
 */
public class ModulusSalt implements Serializable {
  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
  public static final String SEPARATOR = ":";

  private final int modulus;
  private final int digitCount; // used so we get 000, 001, 002 etc

  public ModulusSalt(int modulus) {
    if (modulus<=0) {
      throw new IllegalArgumentException("Modulus must be greater than 0");
    }
    this.modulus = modulus;
    digitCount = ModulusSalt.digitCount(modulus-1); // minus one because e.g. %100 produces 0..99 (2 digits)
  }

  public int saltCharCount() {
    return digitCount;
  }

  /**
   * Provides the salt from the key or throws IAE.
   * @param key To extract the salt from
   * @return The salt as an integer or throws IAE
   */
  public static int saltFrom(String key) {
    try {
      String saltAsString = key.substring(0, key.indexOf(":"));
      return Integer.parseInt(saltAsString);
    } catch (Exception e) {
      throw new IllegalArgumentException("Expected key in form of salt:value (e.g. 123:dataset1). Received: " + key);
    }
  }

  public String saltToString(String key) {
    // positive hashcode values only
    int salt =  (key.hashCode() & 0xfffffff) % modulus;
    return leftPadZeros(salt, digitCount) + SEPARATOR + key;
  }

  /**
   * Pads with 0s to disired length.
   * @param s To pad
   * @param length The final length needed
   * @return The string padded with 0 if needed
   */
  @VisibleForTesting
  static String leftPadZeros(int number, int length) {
    return String.format("%0" + length + "d", number);
  }

  /**
   * Salts the key ready for use as a byte string in HBase.
   * @param key To salt
   * @return The salted key ready for use with HBase.
   */
  public byte[] salt(String key) {
    return saltToString(key).getBytes(UTF8_CHARSET); // Same implementation as HBase code
  }

  /**
   * A utility to provide the region boundaries for preparing tables programatically.
   * @return the structure necessary to programmatically create HBase tables.
   */
  public byte[][] getTableRegions() {
    byte[][] regions = new byte[modulus - 1][digitCount];
    for (int i = 1; i < modulus; i++) {
      regions[i - 1] = leftPadZeros(i, digitCount).getBytes(UTF8_CHARSET);
    }
    return regions;
  }


  /**
   * Returns the number of digits in the number.  This will obly provide sensible results for number>0 and the input
   * is not sanitized.
   *
   * @return the number of digits in the number
   */
  @VisibleForTesting
  static int digitCount(int number) {
    return (int)(Math.log10(number)+1);
  }
}
