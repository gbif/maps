package org.gbif.maps.common.meta;

import java.io.Closeable;
import javax.annotation.Nullable;

/**
 * Provides the ability to read and write metadata about the maps.
 */
public interface MapMetastore extends Closeable {

  /**
   * @return The current map metadata or null of none is available
   */
  @Nullable
  MapTables read() throws Exception;

  /**
   * Updates the metadata overwriting anything that may exist.
   * @param meta To create or replace existing values.
   */
  void update(MapTables meta) throws Exception;
}
