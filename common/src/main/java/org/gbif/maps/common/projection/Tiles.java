package org.gbif.maps.common.projection;

import java.awt.*;
import java.awt.geom.Point2D;

public class Tiles {
  public static TileProjection projectionFrom(String EPSG) {
    return new NorthPoleLAEAEurope(128); //TODO
  }
}
