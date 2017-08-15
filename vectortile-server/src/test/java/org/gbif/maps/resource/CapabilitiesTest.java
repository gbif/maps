package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Long2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CapabilitiesTest {
  private static final GeometryFactory GEOM_FACTORY = new GeometryFactory();
  private static final Logger LOG = LoggerFactory.getLogger(CapabilitiesTest.class);
  private static final int TILE_SIZE = 4096;
  private static final TileProjection PROJ = Tiles.fromEPSG("EPSG:4326", TILE_SIZE);

  // the bounds of the 2 tiles for zoom 0
  private static final Double2D ZOOM_0_WEST_NW = new Double2D(-180, 90);
  private static final Double2D ZOOM_0_WEST_SE = new Double2D(0, -90);
  private static final Double2D ZOOM_0_EAST_NW = new Double2D(0, 90);
  private static final Double2D ZOOM_0_EAST_SE = new Double2D(180, -90);

  @Test
  public void testSimple() throws IOException {
    Capabilities.CapabilitiesBuilder builder = Capabilities.CapabilitiesBuilder.newBuilder();

    // Western tile
    VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, TILE_SIZE/4, false);
    encoder.addFeature("Layer1", ImmutableMap.of("1900", 10, "1910", 20, "total", 30), point(-67d, -124d));
    encoder.addFeature("layer2", ImmutableMap.of("1900", 10, "1930", 10, "total", 20), point(13d, -34.3d));
    builder.collect(encoder.encode(), ZOOM_0_WEST_NW, ZOOM_0_WEST_SE, "2017-08-15T16:28Z");

    Capabilities capabilities = builder.build();
    assertEquals("Western tile failed minLat", -68, capabilities.getMinLat());
    assertEquals("Western tile failed minLng", -125, capabilities.getMinLng());
    assertEquals("Western tile failed maxLat", 14, capabilities.getMaxLat());
    assertEquals("Western tile failed maxLng", -35, capabilities.getMaxLng());
    assertEquals("Western tile failed total", 50, capabilities.getTotal());
    assertEquals("Western tile failed minYear", Integer.valueOf(1900), capabilities.getMinYear());
    assertEquals("Western tile failed maxYear", Integer.valueOf(1930), capabilities.getMaxYear());
    assertEquals("Western tile failed generated", "2017-08-15T16:28Z", capabilities.getGenerated());

    // Eastern tile
    encoder = new VectorTileEncoder(TILE_SIZE, TILE_SIZE/4, false);
    encoder.addFeature("Layer1", ImmutableMap.of("1900", 10, "1910", 20, "total", 30), point(-77.2d, 12d));
    encoder.addFeature("layer3", ImmutableMap.of("1900", 10, "1950", 10, "total", 20), point(17.2d, 13d));
    builder.collect(encoder.encode(), ZOOM_0_EAST_NW, ZOOM_0_EAST_SE, "2017-08-15T16:28Z");

    // now both tiles in the capabilities
    capabilities = builder.build();
    assertEquals("Failed minLat", -78, capabilities.getMinLat());
    assertEquals("Failed minLng", -125, capabilities.getMinLng());
    assertEquals("Failed maxLat", 18, capabilities.getMaxLat());
    assertEquals("Failed maxLng", 14, capabilities.getMaxLng());
    assertEquals("Failed total", 100, capabilities.getTotal());
    assertEquals("Failed minYear", Integer.valueOf(1900), capabilities.getMinYear());
    assertEquals("Failed maxYear", Integer.valueOf(1950), capabilities.getMaxYear());
  }

  @Test
  public void testNoData() throws IOException {
    Capabilities.CapabilitiesBuilder builder = Capabilities.CapabilitiesBuilder.newBuilder();
    Capabilities capabilities = builder.build();

    assertEquals("Failed minLat", -90, capabilities.getMinLat());
    assertEquals("Failed minLng", -180, capabilities.getMinLng());
    assertEquals("Failed maxLat", 90, capabilities.getMaxLat());
    assertEquals("Failed maxLng", 180, capabilities.getMaxLng());
    assertEquals("Failed total", 0, capabilities.getTotal());
    assertTrue("Failed minYear", capabilities.getMinYear() == null);
    assertTrue("Failed maxYear", capabilities.getMaxYear() == null);
  }

  private static Point point(double lat, double lng) {
    Double2D globalXY = PROJ.toGlobalPixelXY(lat, lng, 0);
    Long2D tileXY = Tiles.toTileXY(globalXY, TileSchema.WGS84_PLATE_CAREÉ, 0, TILE_SIZE);
    Double2D tileLocalXY = Tiles.toTileLocalXY(globalXY, TileSchema.WGS84_PLATE_CAREÉ, 0,
                                               tileXY.getX(), tileXY.getY(), TILE_SIZE, TILE_SIZE/4);
    return GEOM_FACTORY.createPoint(new Coordinate(tileLocalXY.getX(),tileLocalXY.getY()));
  }
}
