package org.gbif.maps.resource;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * An encoder that clips points to the buffer extent as if they were polygons, rather than accepting the default
 * behaviour of clipping to the tile extent.
 * @see https://github.com/ElectronicChartCentre/java-vector-tile/issues/12
 */
public class BufferedVectorTileEncoder extends VectorTileEncoder {
  private final Geometry polygonClipGeometry;

  public BufferedVectorTileEncoder(int extent, int polygonClipBuffer, boolean autoScale) {
    super(extent, polygonClipBuffer, autoScale);
    final int size = autoScale ? 256 : extent;
    this.polygonClipGeometry = createTileEnvelope(polygonClipBuffer, size);
  }

  private static Geometry createTileEnvelope(int buffer, int size) {
    Coordinate[] coords = new Coordinate[]{
      new Coordinate((double)(0 - buffer), (double)(size + buffer)),
      new Coordinate((double)(size + buffer), (double)(size + buffer)),
      new Coordinate((double)(size + buffer), (double)(0 - buffer)),
      new Coordinate((double)(0 - buffer), (double)(0 - buffer)), null};
    coords[4] = coords[0];
    return (new GeometryFactory()).createPolygon(coords);
  }

  @Override
  protected Geometry clipGeometry(Geometry geometry) {
    if(geometry instanceof Point) {
      return polygonClipGeometry.intersection(geometry);
    } else {
      return super.clipGeometry(geometry);
    }
  }
}
