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
package org.gbif.maps.resource;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.tags.Tag;

/**
 * Java configuration of the OpenAPI specification.
 */
@Component
public class OpenAPIConfiguration {

  /**
   * Sort tags (sections of the registry documentation) by the order extension, rather than alphabetically.
   *
   * Prefix all paths with "/map" as the context-path of this application is not /.
   *
   * Change ".mvt" → "{format}", so the raster API can be described in the same method.
   */
  @Bean
  public OpenApiCustomizer customizeOpenApi() {
    return openApi -> {
      openApi.setTags(openApi.getTags()
        .stream()
        .sorted(tagOrder())
        .collect(Collectors.toList()));

      Paths paths = openApi.getPaths().entrySet()
        .stream()
        .collect(
          Paths::new,
          (map, item) -> map.addPathItem(
            "/map" + item.getKey().replace(".mvt", "{format}"),
            item.getValue()),
          Paths::putAll);

      openApi.setPaths(paths);
    };
  }

  Comparator<Tag> tagOrder() {
    return Comparator.comparing(tag ->
      tag.getExtensions() == null ?
        "__" + tag.getName() :
        ((Map)tag.getExtensions().get("x-Order")).get("Order").toString());
  }
}
