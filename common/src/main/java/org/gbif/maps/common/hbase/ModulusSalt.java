package org.gbif.maps.common.hbase;

import java.nio.charset.Charset;

import com.google.common.annotations.VisibleForTesting;

/**
 * Defines the salting behavior for encoding keys in HBase.
 * <p/>
 * Salting is used primarily to allow a simple way to create presplit tables with equal key distributions, and as a
 * secondary concern it will likely improve the concurrent read throughput by balancing requests across the region
 * servers.
 */
public class ModulusSalt {
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

  @VisibleForTesting
  String saltToString(String key) {
    int salt =  key.hashCode() % modulus;
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
