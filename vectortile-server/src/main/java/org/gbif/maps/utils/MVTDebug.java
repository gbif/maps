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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import no.ecc.vectortile.VectorTileDecoder;

/**
 * A utility to help diagnose MVTs.
 */
public class MVTDebug {

  public static void main(String[] args) throws IOException {
    Preconditions.checkArgument(args.length == 1, "Args should have a file");
    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false); // important to avoid auto scaling to 256 tiles

    File input = new File(args[0]);
    byte[] bytes = Files.toByteArray(input);
    VectorTileDecoder.FeatureIterable features = decoder.decode(bytes);

    for (String layer : features.getLayerNames()) {
      System.out.println("Layer: " + layer);
    }

    Iterator<VectorTileDecoder.Feature> iter = features.iterator();
    int total = 0;
    int duplicateLocations = 0;
    int verboseAttributes = 0;
    Set<String> coords = Sets.newHashSet();
    while (iter.hasNext()) {
      VectorTileDecoder.Feature f = iter.next();
      String coord = f.getGeometry().toString();
      System.out.println(coord);
      if (coords.contains(coord)) {
        duplicateLocations++;
      }
      if (f.getAttributes().size() > 1) {
        verboseAttributes++;
      };
      total++;
    }
    System.out.println("Duplicate locations: " + duplicateLocations);
    System.out.println("Verbose attributes: " + verboseAttributes);
    System.out.println("Total features: " + total);

  }
}
