package org.gbif.maps.common.filter;

import org.gbif.maps.io.PointFeature;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import no.ecc.vectortile.VectorTileDecoder;

/**
 * A reader of features for the simple points so that they are presented in as the same stream as if they came from
 * a vector tile decoder.
 */
public class FeatureReader {

  public static class PointFeatureIterable implements Iterator<VectorTileDecoder.Feature>  {

    private final Set<String> layers;

    public PointFeatureIterable(PointFeature.PointFeatures features, Set<String> layers) {

      Iterator<PointFeature.PointFeatures.Feature> iter = features.getFeaturesList().iterator();

      //new VectorTileDecoder.Feature();


      //this.features = features;
      this.layers = layers;
    }

    @Override
    public boolean hasNext() {
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public VectorTileDecoder.Feature next() {
      throw new UnsupportedOperationException("Not implemented yet");
    }
  }

}
