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
package org.gbif.maps.common.bin;

import java.io.IOException;
import java.util.Map;

import org.codetome.hexameter.core.backport.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * A utility to deal with the binning of point based vector tiles into square cells representing geometries.
 *
 * Note: This is a rather hastily prepared implementation for a last minute pre-go-live requirement.
 */
public class SquareBin implements Binnable {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final VectorTileDecoder DECODER = new VectorTileDecoder();

  public static final String LAYER_NAME = "occurrence";
  private static final String META_TOTAL_KEY = "total";

  static {
    DECODER.setAutoScale(false);
  }

  private final int tileSize;
  private final int cellSize; // 2x2px would be size 2, 4x4px would be 4 etc
  private final int cellsPerTile;


  /**
   * Constructs binner detailing the number of pixels per square.
   * @param tileSize The tile size to paint
   * @param cellSize The pixels per cell - should be 1,2,4,8,16 etc or strange things will happen
   */
  public SquareBin(int tileSize, int cellSize) {
    this.tileSize = tileSize;
    this.cellSize = cellSize;
    this.cellsPerTile = tileSize / cellSize;
  }

  @Override
  public byte[] bin(byte[] sourceTile, int z, long x, long y) throws IOException {
    VectorTileDecoder.FeatureIterable tile = DECODER.decode(sourceTile, LAYER_NAME);
    Preconditions.checkArgument(tile.getLayerNames().contains(LAYER_NAME), "Tile is missing the expected layer: "
                                                                          + LAYER_NAME);

    // The final data is encoded cellKey -> [YearAsString -> count]
    Map<Long, Long> cells = Maps.newHashMap();

    Iterable<VectorTileDecoder.Feature> features = (Iterable<VectorTileDecoder.Feature>)() -> tile.iterator();
    int scale = 1; // ratio between the supplied tile to the target tile (e.g. 512 -> 4096 = 8)
    for (VectorTileDecoder.Feature feature : features) {

      Geometry geom = feature.getGeometry();
      Preconditions.checkArgument(geom instanceof Point, "Only Point based vector tiles can be binned");
      Point tileLocalXY = (Point) geom;
      scale = (int) ((double)tileSize) / feature.getExtent();

      // skip boundaries
      if (tileContains(feature.getExtent(), tileLocalXY)) {

        long cellID = cellKey(tileLocalXY.getX(), tileLocalXY.getY(), scale);
        long total = cells.getOrDefault(cellID, 0l);
        Optional<Long> cellTotal = totalCount(feature.getAttributes());
        if (cellTotal.isPresent()) {
          total += cellTotal.get();
        }
        cells.put(cellID, total);
      }
    }
    final int scaleFinal = scale;
    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, 0, false); // no buffer because squares tesselate nicely
    cells.forEach((cellID, total) -> {

      Polygon poly = cellToPoly(cellID, scaleFinal);
      Map<String, Object> meta = Maps.newHashMap();
      meta.put(META_TOTAL_KEY, total);
      encoder.addFeature(LAYER_NAME, meta, poly);
    });


    return encoder.encode();
  }

  // clips boundary data
  boolean tileContains(int extent, Point p) {
    return (p.getX()>=0 && p.getY() >= 0 && p.getY() < extent && p.getX() < extent);
  }

  /**
   * @return the cell ID for the pixel at x,y after scaling to the target tile
   */
  long cellKey(double pixelX, double pixelY, int scale) {
    int x = (int) ((pixelX * scale)/cellSize);
    int y = (int) ((pixelY * scale)/cellSize);

    long id = (((long)x) << 32) | (y & 0xffffffffL);
    return id;
  }

  Polygon cellToPoly(long cellID, int scale) {
    int x = (int)(cellID >> 32);
    int y = (int)cellID;

    x *= cellSize;
    y *= cellSize;

    Coordinate[] coordinates = new Coordinate[] {
      new Coordinate(x,y),
      new Coordinate(x + cellSize,y),
      new Coordinate(x + cellSize,y + cellSize),
      new Coordinate(x,y + cellSize),
      new Coordinate(x,y) // closed
    };

    //LOG.info("cellID[{}], scale[{}], cellSize[{}], cellsPerTile[{}], x[{}], y[{}] -> {},{} / {},{}", cellID, scale, cellSize, cellsPerTile, x, y,
    //         coordinates[0].x, coordinates[0].y, coordinates[2].x, coordinates[2].y);

    LinearRing linear = GEOMETRY_FACTORY.createLinearRing(coordinates);
    return new Polygon(linear, null, GEOMETRY_FACTORY);
  }

  /**
   * Leniently attempts to get a total from the meta.
   */
  private Optional<Long> totalCount(Map<String, Object> meta) {
    if (meta != null && meta.containsKey(META_TOTAL_KEY)) {
      try {
        Long total = Long.parseLong(meta.get(META_TOTAL_KEY).toString()); // support anything that can be parsed
        return Optional.of(total);
      } catch (NumberFormatException e) {
        // swallow
      }
    }
    return Optional.empty();
  }
}
