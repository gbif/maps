package org.gbif.maps.common.bin;

import java.io.IOException;

public interface Binnable {

  byte[] bin(byte[] sourceTile, int z, long x, long y) throws IOException;
}
