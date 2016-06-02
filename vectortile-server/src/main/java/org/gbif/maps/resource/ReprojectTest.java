package org.gbif.maps.resource;


import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import javax.measure.unit.SI;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;
import org.geotools.referencing.CRS;
import org.geotools.renderer.lite.RendererUtilities;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.geometry.jts.*;
import org.opengis.referencing.operation.TransformException;

public class ReprojectTest {
  private static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final int TILE_SIZE=4096;

  public static void main(String[] args) throws Exception {
    VectorTileEncoder e = new VectorTileEncoder(TILE_SIZE, 0, false);

    /*
    for (int r = 0; r<=90; r+=10) {
      for (int a = -180; a<=180; a++) {
        e.addFeature("arctic", new HashMap<String, Object>(), toPixel(reproject(r,a)));
      }
    }
    */

    try (
      //BufferedReader reader = new BufferedReader(new FileReader("/Users/tim/dev/git/gbif/maps/vectortile-server/p.csv"))
      BufferedReader reader = new BufferedReader(new FileReader("/Users/tim/dev/git/gbif/maps/vectortile-server/p2.csv"))
      ) {
      String line = reader.readLine();
      line = reader.readLine(); // skip header
      double min = Integer.MAX_VALUE;
      double max = Integer.MIN_VALUE;
      while(line!=null) {
        String[] atoms = line.split(",");
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("count", Integer.parseInt(atoms[2]));

        Point p = reproject(Double.parseDouble(atoms[0]),
                            Double.parseDouble(atoms[1]));
        if (min>p.getX()) min = p.getX();
        if (min>p.getY()) min = p.getY();
        if (max<p.getX()) max = p.getX();
        if (max<p.getY()) max = p.getY();

        Point p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(Double.parseDouble(atoms[1]), Double.parseDouble(atoms[0])));
        //e.addFeature("arctic", meta, toPixel(p1));
        e.addFeature("arctic", meta, toPixel(p));

        //e.addFeature("arctic", meta, toPixel(p));
        line = reader.readLine();
      }
      System.out.println("Minimum seen: " + min);
      System.out.println("Maximum seen: " + max);
    }

    for (int lng = -180; lng <180; lng ++) {
      Point p = reproject(-89.999, lng);
      //e.addFeature("arctic", meta, toPixel(p1));
      Map<String, Object> meta = new HashMap<String, Object>();
      meta.put("count", 1);
      e.addFeature("arctic", meta, toPixel(p));
    }

    CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3575");

    System.out.println(CRS.getGeographicBoundingBox(targetCRS));
    System.out.println(CRS.getProjectedCRS(targetCRS));
    System.out.println(CRS.getEnvelope(targetCRS));


    byte[] data = e.encode();
    try (FileOutputStream stream = new FileOutputStream("/tmp/arctic.mvt")) {
      stream.write(data);
      stream.close();
    }



    /*
    reproject(70, -175);
    reproject(70, -90);
    reproject(70, 0);
    reproject(70, 90);
    reproject(70, 171);

    reproject(60, -180);
    reproject(60, 0);
    reproject(90, 180);
    */
    //reproject(-5, 79);

    System.out.println(reproject(90, 180));
    System.out.println(reproject(0, 180));
    System.out.println(reproject(0, 0));
    System.out.println(reproject(0, -180));

  }



  private static Point reproject(double lat,double lng) throws Exception {



    CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:4326");
    //CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3573");
    //CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3995");
    //CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3413");
    CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3575");

    /*
    System.out.println(CRS.getGeographicBoundingBox(targetCRS));
    System.out.println(CRS.getProjectedCRS(targetCRS));
    System.out.println(CRS.getEnvelope(targetCRS));
    */


    //CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3575");

    // NOTE: Axis order for EPSG:4326 is lat,lng so invert X=latitude, Y=longitude
    // Proven with if( CRS.getAxisOrder(sourceCRS) == CRS.AxisOrder.LAT_LON) {...}
    //Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(lat, lng));

    //GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
    //Point point = geometryFactory.createPoint(new Coordinate( lat, lng));

    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(lat, lng));
    MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, true); // lenient
    Point p2 = (Point) JTS.transform(point, transform);

    //System.out.println(p2.getX() + "," + p2.getY());
    return p2;
  }

  // takes the projected point, and assigns a pixel
  static Point toPixel(Point p) throws FactoryException, TransformException {
    //double max = 3333134.0276;
    //double max = 5050747.2631 * 4;   // 4 seems to work, but why???
    //double max = 9036842.762 * 2;
    // 3333134 = 60 degrees

    /*
    double max = (2349829 * 2) * 4; // Not sure why 4 works...
    double px = 2048 + ((p.getX() / max) * 2048);
    double py = 4096 - (2048 + ((p.getY() / max) * 2048));
    */

    //double max = 2349829.1623399747*8; // I have seen "5,697,676" and "12,255,508" !!!
    double max = 20000000; // I have seen "5,697,676" and "12,255,508" !!!
    double px = (2048*(p.getX()/max)) + 2048;
    double py = 4096-((2048*(p.getY()/max)) + 2048);


    /*
    double max = 2349829.1623399747*8; // Not sure why 4 works...
    double px = 128 + (128 * p.getX() / max);
    double py = 256 - (128 + (128 * p.getY() / max));
    */
    CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3575");
    //CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3413");
    //CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:4326");

    //ReferencedEnvelope re = ReferencedEnvelope.create(targetCRS);
    //ReferencedEnvelope re = ReferencedEnvelope.create(sourceCRS);
    //ReferencedEnvelope re = ReferencedEnvelope.EVERYTHING;
    //ReferencedEnvelope re = new ReferencedEnvelope(-9009964.8, 9009964.8, -9009964.8, 9009964.8, targetCRS);
    //System.out.println(targetCRS.getCoordinateSystem().getAxis(0).getUnit().isCompatible(SI.METER));
    //RendererUtilities.calculateOGCScale

    // or
    //http://gis.stackexchange.com/questions/149440/epsg3575-projected-bounds
    //ReferencedEnvelope re = new ReferencedEnvelope(-9009964.8, 9009964.8, -9009964.8, 9009964.8, targetCRS);
    //ReferencedEnvelope re = new ReferencedEnvelope(-12742014.4, 12742014.4, -12742014.4, 12742014.4, targetCRS);
    //double m  = 9009964.8 * 2;
    //double m = 40075160/2;  // works, but why???
    //double m  = 12742014.4 * 2;
    //ReferencedEnvelope re = new ReferencedEnvelope(-m, m, -m, m, targetCRS);
    //double m = 382558.89;
    //ReferencedEnvelope re = new ReferencedEnvelope(-m, m, -m, m, targetCRS);

    //Envelope e = new Envelope(-180, 180.0, 0, 90);
    // Envelope e = new Envelope(-2349829.1623399747, 2349829.1623399747, -2349829.1623399747, 2349829.1623399747);

    //Rectangle r = new Rectangle(0, 0, TILE_SIZE, TILE_SIZE);
    //AffineTransform t = RendererUtilities.worldToScreenTransform(re, r, targetCRS);



    //This works
    /*
    double m = 40075160/2;  // works, but why???
    ReferencedEnvelope re = new ReferencedEnvelope(-m, m, -m, m, targetCRS);
    Rectangle r = new Rectangle(0, 0, TILE_SIZE, TILE_SIZE);
    AffineTransform t = RendererUtilities.worldToScreenTransform(re, r, targetCRS);
    System.out.println(t);
    */

    double r = 40075160;  // circumference of the equator in meters

    // move world coorindates into positive space addressing, so the lowest is 0,0
    AffineTransform translate= AffineTransform.getTranslateInstance(r/2, r/2);
    double pixelsPerMeter = ((double)TILE_SIZE) / r;
    AffineTransform scale= AffineTransform.getScaleInstance(pixelsPerMeter, pixelsPerMeter);
    // Swap Y to convert world addressing to pixel addressing where 0,0 is at the top
    AffineTransform mirror_y = new AffineTransform(1, 0, 0, -1, 0, TILE_SIZE);

    // combine the transform, noting you reverse the order
    AffineTransform world2pixel = new AffineTransform(mirror_y);
    world2pixel.concatenate(scale);
    world2pixel.concatenate(translate);


    //AffineTransform t = RendererUtilities.worldToScreenTransform(re, r, targetCRS);


    Point2D src = new Point2D.Double(p.getX(), p.getY());
    Point2D tgt = new Point2D.Double();
    world2pixel.transform(src,tgt);

    //return GEOMETRY_FACTORY.createPoint(new Coordinate(px,py));
    System.out.println(src + " -> " + tgt);
    return GEOMETRY_FACTORY.createPoint(new Coordinate(tgt.getX(),tgt.getY()));
  }

}
