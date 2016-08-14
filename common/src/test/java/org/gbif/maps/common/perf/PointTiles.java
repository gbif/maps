package org.gbif.maps.common.perf;

import org.gbif.maps.common.projection.Int2D;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.protobuf.nano.InvalidProtocolBufferNanoException;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectLists;
import vector_tile.VectorTile;

/**
 * An insanely optimized implementation of a point tile reader.
 */
public class PointTiles {

  private static int zigZagDecode(int n) {
    return ((n >> 1) ^ (-(n & 1)));
  }

  public static List<Feature> parsePoints(byte[] encoded) throws InvalidProtocolBufferNanoException {
    List<Feature> result = new ObjectArrayList<Feature>(10000);
    VectorTile.Tile tile = VectorTile.Tile.parseFrom(encoded);

    for (VectorTile.Tile.Layer layer : tile.layers) {

      // Convert the values to usable types
      Object[] values = new Object[layer.values.length];
      int i = 0;
      for (VectorTile.Tile.Value value : layer.values) {
        if (value.hasIntValue()) {
          values[i++] = value.getIntValue();
        } else if (value.hasSintValue()) {
          values[i++] = value.getSintValue();
        } else if (value.hasUintValue()) {
          values[i++] = value.getUintValue();
        } else if (value.hasBoolValue()) {
          values[i++] = value.getBoolValue();
        } else if (value.hasDoubleValue()) {
          values[i++] = value.getDoubleValue();
        } else if (value.hasFloatValue()) {
          values[i++] = value.getFloatValue();
        }  else if (value.hasStringValue()) {
          values[i++] = value.getStringValue();
        } else {
          values[i++] = null;
        }
      }

      for (VectorTile.Tile.Feature feature : layer.features) {
        int px = zigZagDecode(feature.geometry[1]); // geometry[0] states the length.  We require single point(!)
        int py = zigZagDecode(feature.geometry[2]);

        int tagsCount = feature.tags.length;
        Map<String, Object> attributes = new Object2ObjectOpenHashMap(tagsCount / 2);
        int tagIdx = 0;
        while (tagIdx < feature.tags.length) {
          String key = layer.keys[feature.tags[tagIdx++]];
          Object value = values[feature.tags[tagIdx++]];
          attributes.put(key, value);
        }

        result.add(new Feature(layer.name, new Int2D(px,py), attributes));
      }
    }

    return result;
  }


  public static class Feature {
    private final String layer;
    private final Int2D pixel;
    private final Map<String,Object> attributes;

    public Feature(String layer, Int2D pixel, Map<String, Object> attributes) {
      this.layer = layer;
      this.pixel = pixel;
      this.attributes = attributes;
    }

    public Int2D getPixel() {
      return pixel;
    }

    public Map<String, Object> getAttributes() {
      return attributes;
    }

    public String getLayer() {
      return layer;
    }
  }

}
