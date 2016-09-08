package org.gbif.maps.common.bin;

import java.util.Map;

import com.google.common.collect.Maps;
import org.codetome.hexameter.core.api.defaults.DefaultSatelliteData;

public class HexagonData extends DefaultSatelliteData {
  private final Map<String, Object> metadata = Maps.newHashMap();

  public Map<String, Object> getMetadata() {
    return metadata;
  }
}
