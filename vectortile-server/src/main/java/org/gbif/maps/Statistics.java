package org.gbif.maps;

import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;

import java.util.stream.Collector;

/**
 *
 */
public class Statistics {
  private double minX = Double.NaN, minY = Double.NaN, maxX = Double.NaN, maxY = Double.NaN;
  private long count = 0;
  private int extent;

  public void accept(VectorTileDecoder.Feature f) {
    Point p = ((Point)f.getGeometry());

    if (extent == 0) {
      // Extent can be 512 or 4096, depending on Points or Tile source.  We should change the 512px tiles to 4096, since
      // 4096 is standard.
      extent = f.getExtent();
      System.out.println("Extent " + extent);
    }

    boolean within = p.getX() >= 0 && p.getX() < extent
        && p.getY() >= 0 && p.getY() < extent;

//      System.out.println(within + ": " + (p.getX() * (4096 / extent)) + "," + (p.getY() * (4096 / extent)) + "," + f.getAttributes().get("total"));

    if (within) {

      if (count == 0) {
        minX = maxX = p.getX() * (4096/extent);
        minY = maxY = p.getY() * (4096/extent);
      } else {
        minX = Math.min(minX, p.getX() * (4096/extent));
        maxX = Math.max(maxX, p.getX() * (4096/extent));
        minY = Math.min(minY, p.getY() * (4096/extent));
        maxY = Math.max(maxY, p.getY() * (4096/extent));
      }

      // key must be "total"
      count += Long.valueOf(f.getAttributes().get("total").toString());
    }
  }

  public Statistics combine(Statistics b) {
    // TODO
    throw new RuntimeException("Not implemented, but needs doing.");
  }

  public void combineWestAndEast(Statistics east) {
    //LOG.info("West Bounds x{}→{}, y{}→{}", this.minX, this.maxX, this.minY, this.maxY);
    //LOG.info("East Bounds x{}→{}, y{}→{}", east.minX, east.maxX, east.minY, east.maxY);

    if (east.count > 0 && this.count > 0) {
      this.minX = Math.min(this.minX, 4096 + east.minX);
      this.maxX = Math.max(this.maxX, 4096 + east.maxX);
      this.minY = Math.min(this.minY, east.minY);
      this.maxY = Math.max(this.maxY, east.maxY);

      this.count = this.count + east.count;
    } else if (east.count > 0) {
      this.minX = east.minX;
      this.maxX = east.maxX;
      this.minY = east.minY;
      this.maxY = east.maxY;
      this.count = east.count;
    }

    //LOG.info("Bounds x{}→{}, y{}→{}", this.minX, this.maxX, this.minY, this.maxY);
  }

  public Collector<VectorTileDecoder.Feature, Statistics, Statistics> featureCollector()
  {
    return Collector.of(
        ()->new Statistics(),
        Statistics::accept,
        Statistics::combine,
        // TODO: Check these, what does IDENTITY_FINISH mean?
        Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH
    );
  }

  // +1 for maxX/Y because it's been rounded to an integer.
  public double getNorth() {
    return 90 - (minY) / (4096) * 180;
  }
  
  public double getEast() { 
    return -180 + (maxX+1) / (2*4096) * 360;
  }
  
  public double getSouth() { 
    return 90 - (maxY+1) / (  4096) * 180;
  }
  
  public double getWest() { 
    return -180 + (minX) / (2*4096) * 360;
  }
  
  public long getCount() { 
    return count;
  }

  // Obviously temporary...
  public String getGeoJSON() {
    String polygon = String.format("[%f,%f],[%f,%f],[%f,%f],[%f,%f]",
        getWest(), getSouth(),
        getEast(), getSouth(),
        getEast(), getNorth(),
        getWest(), getNorth());

    return "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[["+polygon+"]]},\"properties\":{\"total\":"+count+"}}";
  }

  // But useful, since GeoJSON can be shown very easily in OpenLayers:
  //new ol.layer.Vector({
  //  source: new ol.source.Vector({
  //		url: 'http://mblissett-linux.gbif.org:7001/map/occurrence/density/.json?taxonKey=2926240',
  //		format: new ol.format.GeoJSON()
  //	}),
  //  style: createStatsStyle()
  //});
}
