package org.gbif.maps.common.perf;

  import java.io.IOException;
  import java.util.ArrayList;
  import java.util.Arrays;
  import java.util.Collection;
  import java.util.Collections;
  import java.util.HashMap;
  import java.util.HashSet;
  import java.util.Iterator;
  import java.util.List;
  import java.util.Map;
  import java.util.NoSuchElementException;
  import java.util.Set;

  import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
  import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
  import it.unimi.dsi.fastutil.objects.ObjectArrayList;
  import it.unimi.dsi.fastutil.objects.ObjectList;
  import no.ecc.vectortile.Filter;
  import vector_tile.VectorTile;
  import vector_tile.VectorTile.Tile.Layer;

  import com.vividsolutions.jts.geom.Coordinate;
  import com.vividsolutions.jts.geom.Geometry;
  import com.vividsolutions.jts.geom.GeometryFactory;
  import com.vividsolutions.jts.geom.LineString;
  import com.vividsolutions.jts.geom.LinearRing;

public class FastVectorTileDecoder {

  private boolean autoScale = true;

  /**
   * Get the autoScale setting.
   *
   * @return autoScale
   */
  public boolean isAutoScale() {
    return autoScale;
  }

  /**
   * Set the autoScale setting.
   *
   * @param autoScale
   *            when true, the encoder automatically scale and return all coordinates in the 0..255 range.
   *            when false, the encoder returns all coordinates in the 0..extent-1 range as they are encoded.
   *
   */
  public void setAutoScale(boolean autoScale) {
    this.autoScale = autoScale;
  }

  public FeatureIterable decode(byte[] data) throws IOException {
    return decode(data, Filter.ALL);
  }

  public FeatureIterable decode(byte[] data, String layerName) throws IOException {
    return decode(data, new Filter.Single(layerName));
  }

  public FeatureIterable decode(byte[] data, Set<String> layerNames) throws IOException {
    return decode(data, new Filter.Any(layerNames));
  }

  public FeatureIterable decode(byte[] data, Filter filter) throws IOException {
    VectorTile.Tile tile = VectorTile.Tile.parseFrom(data);
    return new FeatureIterable(tile, filter, autoScale);
  }

  static int zigZagDecode(int n) {
    return ((n >> 1) ^ (-(n & 1)));
  }

  public static final class FeatureIterable implements Iterable<Feature> {

    private final VectorTile.Tile tile;
    private final Filter filter;
    private boolean autoScale;

    public FeatureIterable(VectorTile.Tile tile, Filter filter, boolean autoScale) {
      this.tile = tile;
      this.filter = filter;
      this.autoScale = autoScale;
    }

    public Iterator<Feature> iterator() {
      return new FeatureIterator(tile, filter, autoScale);
    }

    public List<Feature> asList() {
      List<Feature> features = new ArrayList<FastVectorTileDecoder.Feature>();
      for (Feature feature : this) {
        features.add(feature);
      }
      return features;
    }

    public Collection<String> getLayerNames() {
      Set<String> layerNames = new HashSet<String>();
      for (VectorTile.Tile.Layer layer : tile.layers) {
        layerNames.add(layer.name);
      }
      return Collections.unmodifiableSet(layerNames);
    }

  }

  private static final class FeatureIterator implements Iterator<Feature> {

    private final GeometryFactory gf = new GeometryFactory();

    private final Filter filter;

    private final Iterator<VectorTile.Tile.Layer> layerIterator;

    private Iterator<VectorTile.Tile.Feature> featureIterator;

    private int extent;
    private String layerName;
    private double scale;
    private boolean autoScale;

    //private final List<String> keys = new ObjectArrayList<>();
    //private final List<Object> values = new ObjectArrayList<>();
    private String[] keys;
    private Object[] values;

    private Feature next;

    public FeatureIterator(VectorTile.Tile tile, Filter filter, boolean autoScale) {
      layerIterator = Arrays.asList(tile.layers).iterator();
      this.filter = filter;
      this.autoScale = autoScale;
    }

    public boolean hasNext() {
      findNext();
      return next != null;
    }

    public Feature next() {
      findNext();
      if (next == null) {
        throw new NoSuchElementException();
      }
      Feature n = next;
      next = null;
      return n;
    }

    private void findNext() {

      if (next != null) {
        return;
      }

      while (true) {

        if (featureIterator == null || !featureIterator.hasNext()) {
          if (!layerIterator.hasNext()) {
            next = null;
            break;
          }

          Layer layer = layerIterator.next();
          if (!filter.include(layer.name)) {
            continue;
          }

          parseLayer(layer);
          continue;
        }

        next = parseFeature(featureIterator.next());
        break;

      }

    }

    private void parseLayer(VectorTile.Tile.Layer layer) {

      layerName = layer.name;
      extent = layer.getExtent();
      scale = autoScale ? extent / 256.0 : 1.0;

      //keys.clear();
      //keys.addAll(Arrays.asList(layer.keys));
      keys = layer.keys;
      //values.clear();

      values = new Object[layer.values.length];

      int i = 0;
      for (VectorTile.Tile.Value value : layer.values) {
        if (value.hasBoolValue()) {
          values[i++] = value.getBoolValue();
        } else if (value.hasDoubleValue()) {
          values[i++] = value.getDoubleValue();
        } else if (value.hasFloatValue()) {
          values[i++] = value.getFloatValue();
        } else if (value.hasIntValue()) {
          values[i++] = value.getIntValue();
        } else if (value.hasSintValue()) {
          values[i++] = value.getSintValue();
        } else if (value.hasUintValue()) {
          values[i++] = value.getUintValue();
        } else if (value.hasStringValue()) {
          values[i++] = value.getStringValue();
        } else {
          values[i++] = null;
        }

      }

      featureIterator = Arrays.asList(layer.features).iterator();
    }

    private Feature parseFeature(VectorTile.Tile.Feature feature) {

      int tagsCount = feature.tags.length;
      Object2ObjectMap attributes = new Object2ObjectOpenHashMap(tagsCount / 2);
      //Map<String, Object> attributes = new HashMap<String, Object>(tagsCount / 2);
      int tagIdx = 0;
      while (tagIdx < feature.tags.length) {
        String key = keys[feature.tags[tagIdx++]];
        Object value = values[feature.tags[tagIdx++]];
        attributes.put(key, value);
      }

      // Optimisation: Will only work for SINGLE point features
      Geometry geometry = gf.createPoint(new Coordinate(zigZagDecode(feature.geometry[1]), zigZagDecode(feature.geometry[2])));

      return new Feature(layerName, extent, geometry, Collections.unmodifiableMap(attributes));
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  public static final class Feature {

    private final String layerName;
    private final int extent;
    private final Geometry geometry;
    private final Map<String, Object> attributes;

    public Feature(String layerName, int extent, Geometry geometry, Map<String, Object> attributes) {
      this.layerName = layerName;
      this.extent = extent;
      this.geometry = geometry;
      this.attributes = attributes;
    }

    public String getLayerName() {
      return layerName;
    }

    public int getExtent() {
      return extent;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    public Map<String, Object> getAttributes() {
      return attributes;
    }

  }

}
