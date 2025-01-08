package org.gbif.maps.resource;

import org.gbif.maps.common.filter.Range;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Set;

/**
 * Interface to retrieve vector tiles from a database.
 */
public interface TileMaps {

  /**
   * Retrieves the data from database (HBase / Clickhouse), applies the filters and merges the result into a vector tile containing a single
   * layer named {@link TileResource#LAYER_OCCURRENCE}.
   * <p>
   * This method handles both pre-tiled and simple feature list stored data, returning a consistent format of vector
   * tile regardless of the storage format.  Please note that the tile size can vary and should be inspected before use.
   *
   * @param z The zoom level
   * @param x The tile X address for the vector tile
   * @param y The tile Y address for the vector tile
   * @param mapKey The map key being requested
   * @param srs The SRS of the requested tile
   * @param basisOfRecords To include in the filter.  An empty or null value will include all values
   * @param years The year range to filter.
   * @param verbose If true, the vector tile will contain record counts per year
   * @param tileSize The size of the tile in pixels
   * @param bufferSize The size of the buffer in pixels
   * @return DatedVectorTile containing the vector tile data
   * @throws IOException Only if the data in database is corrupt and cannot be decoded.  This is fatal.
   */
  DatedVectorTile filteredVectorTile(int z, long x, long y, String mapKey, String srs,
                                     @Nullable Set<String> basisOfRecords,
                                     @NotNull Range years,
                                     boolean verbose,
                                     Integer tileSize,
                                     Integer bufferSize) throws IOException;

}
