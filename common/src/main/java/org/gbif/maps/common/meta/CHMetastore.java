package org.gbif.maps.common.meta;

import java.io.Closeable;

public interface CHMetastore extends Closeable {

  String getClickhouseDB();
  void setClickhouseDB(String db) throws Exception;
}
