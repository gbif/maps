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
package org.gbif.maps.common.bin;

import java.util.Map;

import org.codetome.hexameter.core.api.defaults.DefaultSatelliteData;

import com.google.common.collect.Maps;

public class HexagonData extends DefaultSatelliteData {
  private final Map<String, Object> metadata = Maps.newHashMap();

  public Map<String, Object> getMetadata() {
    return metadata;
  }
}
