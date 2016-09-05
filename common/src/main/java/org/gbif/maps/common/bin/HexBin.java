package org.gbif.maps.common.bin;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.codetome.hexameter.core.api.DefaultSatelliteData;
import org.codetome.hexameter.core.api.Hexagon;
import org.codetome.hexameter.core.api.HexagonOrientation;
import org.codetome.hexameter.core.api.HexagonalGrid;
import org.codetome.hexameter.core.api.HexagonalGridBuilder;
import org.codetome.hexameter.core.api.HexagonalGridLayout;
import org.codetome.hexameter.core.api.SatelliteData;
import org.codetome.hexameter.core.backport.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to deal with the binning of point based vector tiles into hexagon based vector tiles.
 * <p/>
 * Of critical importance is that the supplied tile has a buffer zone sufficiently large to accommodate the overlap
 * of hexagons across tile boundaries.
 * <p/>
 * See <a href="http://www.redblobgames.com/grids/hexagons">the excellent www.redblobgames.com</a> site for the math
 * surrounding hexagons which was used for this implementation.
 */
public class HexBin {
  private static final Logger LOG = LoggerFactory.getLogger(HexBin.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final VectorTileDecoder DECODER = new VectorTileDecoder();

  private static final int NUM_HEX_BUFFER = 2;

  static {
    DECODER.setAutoScale(false);
  }

  private final int tileSize;
  private final int hexPerTile;


  private final double radius;
  private final double hexWidth;
  private final double hexHeight;

  // TODO: needs to be a common schema thing
  private final String layerName = "occurrence";

  public HexBin(int tileSize, int hexPerTile) {
    this.tileSize = tileSize;
    this.hexPerTile = hexPerTile;

    // in order to teselate, we need to add an extra 1/2 to the user requested tiles.
    // e.g. 9 will result in 9.5 tiles painted
    double w = 1.5 * (((double)hexPerTile + 1) / 2);
    hexWidth = tileSize / w;
    radius = hexWidth / 2;


    // for flat top, 2.5x width produces 3 hexagons, therefore 5R gives 3 hexagons
    //radius = (3.0 * tileSize) / (5d * ((double)hexPerTile + 0.5));
    //hexWidth = radius*2;
    hexHeight = (Math.sqrt(3)/2) * hexWidth;

    LOG.info("Radius [{}], width[{}], height[{}]", radius, hexWidth, hexHeight);

    LOG.info("horizontal: {}" , tileSize / hexWidth);


  }

  public byte[] bin(byte[] sourceTile, int z, long x, long y) throws IOException {
    VectorTileDecoder.FeatureIterable tile = DECODER.decode(sourceTile, layerName);
    Preconditions.checkArgument(tile.getLayerNames().contains(layerName), "Tile is missing the expected layer: "
                                                                          + layerName);
    Iterable<VectorTileDecoder.Feature> features = () -> tile.iterator();

    HexagonalGrid grid = newGridInstance();

    // Hexagons do not align at boundaries, and therefore we need to determine the offsets to ensure polygons
    // meet correctly across tiles.  The maximum offset is 1.5 cells horizontally and 1 cell vertically due to using
    // flat top tiles.  This is apparent when you see a picture. See http://www.redblobgames.com/grids/hexagons/#basics
    final double gridOffsetX = (x*((tileSize)%(1.5*hexWidth)))%(1.5*hexWidth);
    final double gridOffsetY = (y*(tileSize%hexHeight))%hexHeight;

    LOG.info("Grid offsets: {},{}", gridOffsetX, gridOffsetY);

    // for each feature returned from the datastore locate its hexagon and store the data on the hexagon
    Set<Hexagon> dataCells = Sets.newHashSet();
    for (VectorTileDecoder.Feature feature : features) {
      double scale = ((double)tileSize) / feature.getExtent(); // adjust for differing tile sizes
      LOG.debug("Scaling from {} to {} with {}", feature.getExtent(), tileSize, scale);
      Geometry geom = feature.getGeometry();
      Preconditions.checkArgument(geom instanceof Point, "Only Point based vector tiles can be binned");
      Point tileLocalXY = (Point) geom;

      // collect the point features into the Hexagons, noting that we apply the scale to compensate for possible
      // different tile sizes
      Hexagon hex = addFeatureInHex(z,
                                    grid,
                                    gridOffsetX,
                                    gridOffsetY,
                                    tileLocalXY.getX() * scale,
                                    tileLocalXY.getY() * scale,
                                    feature);
      if (hex != null) {
        dataCells.add(hex);
      }

    }

    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, 64, false);
    for (Hexagon hexagon : dataCells) {
      Coordinate[] coordinates = new Coordinate[7];
      int i = 0;
      for (org.codetome.hexameter.core.api.Point point : hexagon.getPoints()) {
        coordinates[i++] = new Coordinate(point.getCoordinateX() - gridOffsetX - (hexWidth * 1.5),
                                          point.getCoordinateY() - gridOffsetY - (2 * hexHeight));
//        coordinates[i++] = new Coordinate(point.getCoordinateX() - gridOffsetX,
//                                          point.getCoordinateY() - gridOffsetY);
      }
      coordinates[6] = coordinates[0]; // close our polygon
      LinearRing linear = GEOMETRY_FACTORY.createLinearRing(coordinates);
      Polygon poly = new Polygon(linear, null, GEOMETRY_FACTORY);

      Map<String, Object> meta = Maps.newHashMap();

      // HACK: a test id
      meta.put("id",
               roundThreeDecimals(hexagon.getCenterY())
               + "," +
               roundThreeDecimals(hexagon.getCenterX())
      );

      if (hexagon.getSatelliteData().isPresent()
          && hexagon.getSatelliteData().get().getCustomData("total").isPresent()) {
        meta.put("total", hexagon.getSatelliteData().get().getCustomData("total").get());
        LOG.debug("total {}", meta.get("total"));

      }

      LOG.debug("Coords {},{},{},{},{},{}" + coordinates[0],
               coordinates[1],
               coordinates[2],
               coordinates[3],
               coordinates[4],
               coordinates[5]);
      encoder.addFeature("occurrence", meta, poly);
    }

    return encoder.encode();
  }

  @VisibleForTesting
  HexagonalGrid newGridInstance() {
    // Set up the NxM grid of hexes, allowing for buffer of 2 hexagons all around.  If hexagons aligned perfectly
    // to the tile boundary a buffer of 1 would suffice.  However, a buffer of 2 allows us to move the grid to align
    // the hexagon polygons with the ones in the tile directly above and to the left.
    int requiredWidth = hexPerTile + NUM_HEX_BUFFER*2;
    int requiredHeight = (int) Math.ceil(tileSize / hexHeight) + NUM_HEX_BUFFER*2;

    LOG.info("Grid: {}x{}", requiredWidth, requiredHeight);

    return new HexagonalGridBuilder()
      .setGridWidth(requiredWidth)
      .setGridHeight(requiredHeight)
      .setGridLayout(HexagonalGridLayout.RECTANGULAR)
      .setOrientation(HexagonOrientation.FLAT_TOP)
      .setRadius(radius)
      .build();
  }

  /**
   * Adds a feature to the satellite data in the hexagon taking into account the offsets.
   * It should be noted that on a tile with 0 offset, the top left of the tile is actually covered by tile 1,0 (x,y)
   * and numbered on an odd-q vertical layout addressing scheme on http://www.redblobgames.com/grids/hexagons/.
   * @param z the zoom
   * @param hexWidth the width of a hexagon
   * @param hexHeight the height of a hexagon
   * @param minTilePixelX the minimum pixel X of the tile in world space
   * @param minTilePixelY the minimum pixel Y of the tile in world space
   * @param grid the hexagon grid
   * @param gridOffsetX the offset for the hexagon to align with adjacent tiles
   * @param gridOffsetY the offset for the hexagon to align with adjacent tiles
   * @param feature to inspect and add
   * @return the hexagon or null when the hexagon is not on the hex grid or if satellite data is null and it cannot be
   * created.
   */
  private Hexagon addFeatureInHex(
    int z,
    HexagonalGrid grid,
    double gridOffsetX,
    double gridOffsetY,
    double tileLocalX,
    double tileLocalY,
    VectorTileDecoder.Feature feature
  ) {

    // and the pixel when on hex grid space, compensating for the offset and 2 hex buffer
    // TODO: here we need to use NUM_HEX_BUFFER
    double hexGridLocalX = tileLocalX + gridOffsetX + 1.5*hexWidth;
    double hexGridLocalY = tileLocalY + gridOffsetY + 2*hexHeight;
    //double hexGridLocalX = tileLocalX;
    //double hexGridLocalY = tileLocalY;



    Optional<Hexagon> hex = grid.getByPixelCoordinate(hexGridLocalX, hexGridLocalY);
    if (hex.isPresent()) {
      Hexagon hexagon = hex.get();

      if (!hexagon.getSatelliteData().isPresent()) {
        hexagon.setSatelliteData(new DefaultSatelliteData());
      }

      if (hexagon.getSatelliteData().isPresent()) {
        SatelliteData cellData = hexagon.getSatelliteData().get();

        // HACK!!!
        long total = 10;
        if (!cellData.getCustomData("total").isPresent()) {
          cellData.addCustomData("total", total);
        } else {
          long existing = (Long)cellData.getCustomData("total").get();
          cellData.addCustomData("total", total + existing);
        }
      }
      return hexagon;
    }

    return null;
  }

  /**
   * Rounds to 3 decimal places
   */
  private static double roundThreeDecimals(double d) {
    DecimalFormat df = new DecimalFormat("#.###");
    return Double.valueOf(df.format(d));
  }


}
