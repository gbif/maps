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
package org.gbif.maps.utils;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.imageio.ImageIO;

import org.locationtech.jts.geom.Point;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import no.ecc.vectortile.VectorTileDecoder;

/**
 * A utility to help diagnose MVTs by creating a PNG.
 * Usage: MVT2PNG source.mvt dest.png
 *
 * Get a tile:
 * - http://api-es.gbif-uat.org/v2/map/occurrence/adhoc/0/0/0.mvt?srs=EPSG%3A4326&mode=GEO_CENTROID&tileBuffer=0.25
 * - http://api-es.gbif-uat.org/v2/map/occurrence/adhoc/1/1/0.mvt?srs=EPSG%3A4326&mode=GEO_CENTROID&tileBuffer=0.25
 *
 */
public class MVT2PNG {

  public static void main(String[] args) throws IOException {
    Preconditions.checkArgument(args.length == 2, "Args should have 2 files");

    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false); // important to avoid auto scaling to 256 tiles

    File input = new File(args[0]);
    byte[] bytes = Files.toByteArray(input);
    VectorTileDecoder.FeatureIterable features = decoder.decode(bytes);


    Iterator<VectorTileDecoder.Feature> iter = features.iterator();

    Set<VectorTileDecoder.Feature> coords = Sets.newHashSet();
    int extent = 512;
    double minX = 0, maxX = 512, minY = 0, maxY = 512;

    while (iter.hasNext()) {
      VectorTileDecoder.Feature f = iter.next();
      coords.add(f);
      extent = f.getExtent();

      minX = Math.min(minX, ((Point)f.getGeometry()).getX());
      maxX = Math.max(maxX, ((Point)f.getGeometry()).getX());
      minY = Math.min(minY, ((Point)f.getGeometry()).getY());
      maxY = Math.max(maxY, ((Point)f.getGeometry()).getY());
    }

    int width = 1 + (int)maxX - (int)minX;
    int height = 1 + (int)maxY - (int)minY;
    BufferedImage bi = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    Graphics2D ig2 = bi.createGraphics();


    int offsetX = (int)Math.abs(minX);
    int offsetY = (int)Math.abs(minY);

    System.out.println(minX + ":" + minY);
    ig2.setPaint(Color.blue);
    ig2.draw3DRect(offsetX, offsetY, extent, extent, false);

    ig2.setPaint(Color.green);
    ig2.setBackground(Color.green);
    for (VectorTileDecoder.Feature p : coords) {

      long count = (Long) p.getAttributes().get("total");

      int size = 4;
      ig2.setColor(Color.decode("0xfed976"));

      if (count >= 10) {size = 6; ig2.setColor(Color.decode("0xfd8d3c"));}
      if (count >= 100) {size = 10; ig2.setColor(Color.decode("0xfd8d3c"));}
      if (count >= 1000) {size = 16; ig2.setColor(Color.decode("0xf03b20"));}
      if (count >= 10000) {size = 30; ig2.setColor(Color.decode("0xbd0026"));}

      Point g = (Point)p.getGeometry();

      ig2.fill(new Ellipse2D.Double (offsetX + (int)g.getX(), offsetY + (int) g.getY(), size, size));
    }

    ImageIO.write(bi, "PNG", new File(args[1]));


  }
}
