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

import com.codahale.metrics.annotation.Timed;
import io.swagger.v3.oas.annotations.Hidden;
import org.gbif.maps.common.projection.SphericalMercator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

import static org.gbif.maps.resource.Params.ALL_MAP_KEY;
import static org.gbif.maps.resource.Params.MAP_TYPES;
import static org.gbif.maps.resource.Params.enableCORS;
import static org.gbif.maps.resource.TileResource.ZOOM_0_EAST_NW;
import static org.gbif.maps.resource.TileResource.ZOOM_0_EAST_SE;
import static org.gbif.maps.resource.TileResource.ZOOM_0_WEST_NW;
import static org.gbif.maps.resource.TileResource.ZOOM_0_WEST_SE;

/**
 * Provide backward compatibility for the /v1/map/density/tile.json API call.
 *
 * Ignore layer and year handling, the logs suggest this isn't used, although the V1 tile-server does support it.
 * (Code possibly implementing it anyway was removed on 2017-08-22.)
 */
@Hidden
@RestController
@RequestMapping(
  value = "density"
)
@Profile("!es-only")
public class BackwardCompatibility {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardCompatibility.class);

  private final TileResource tileResource;

  /**
   * Construct the resource
   */
  @Autowired
  public BackwardCompatibility(TileResource tileResource) throws Exception {
    this.tileResource = tileResource;
  }

  /**
   * Returns a capabilities response with the extent and year range built by inspecting the zoom 0 tiles of the
   * EPSG:4326 projection.
   */
  @RequestMapping(
    method = RequestMethod.GET,
    value = "/tile.json",
    produces = MediaType.APPLICATION_JSON_VALUE
  )
  @Timed
  public V1TileJson tileJson(
      @RequestParam("type") String type,
      @RequestParam("key") String key,
      @RequestParam("layers") List<String> layers,
      HttpServletResponse response)
      throws Exception {

    enableCORS(response);

    String mapKey;

    switch (type) {
      case "TAXON":
        mapKey = MAP_TYPES.get("taxonKey") + ":" + key;
        break;
      case "DATASET":
        mapKey = MAP_TYPES.get("datasetKey") + ":" + key;
        break;
      case "PUBLISHER":
        mapKey = MAP_TYPES.get("publishingOrg") + ":" + key;
        break;
      case "COUNTRY":
        mapKey = MAP_TYPES.get("country") + ":" + key;
        break;
      case "PUBLISHING_COUNTRY":
        mapKey = MAP_TYPES.get("publishingCountry") + ":" + key;
        break;
      default:
        mapKey = ALL_MAP_KEY;
    }

    Capabilities.CapabilitiesBuilder builder = Capabilities.CapabilitiesBuilder.newBuilder();
    DatedVectorTile west = tileResource.getTile(0,0,0,mapKey,null,"EPSG:4326",null,null,true,null,0,0);
    DatedVectorTile east = tileResource.getTile(0,1,0,mapKey,null,"EPSG:4326",null,null,true,null,0,0);
    builder.collect(west.tile, ZOOM_0_WEST_NW, ZOOM_0_WEST_SE, west.date);
    builder.collect(east.tile, ZOOM_0_EAST_NW, ZOOM_0_EAST_SE, east.date);
    Capabilities capabilities = builder.build();
    LOG.info("Capabilities for v1 tile.json: {}", capabilities);

    if (capabilities.getGenerated() != null) {
      response.setHeader("ETag", String.format("\"%s\"", capabilities.getGenerated()));
    }

    return new V1TileJson(capabilities);
  }

  class V1TileJson {
    final long count;
    final double minimumLatitude;
    final double minimumLongitude;
    final double maximumLatitude;
    final double maximumLongitude;

    V1TileJson(Capabilities c) {
      this.count = c.getTotal();
      this.minimumLatitude = Math.max(-SphericalMercator.MAX_LATITUDE, c.getMinLat());
      this.maximumLatitude = Math.min( SphericalMercator.MAX_LATITUDE, c.getMaxLat());
      this.minimumLongitude = c.getMinLng();
      this.maximumLongitude = c.getMaxLng();
    }

    public long getCount() {
      return count;
    }

    public double getMinimumLatitude() {
      return minimumLatitude;
    }

    public double getMinimumLongitude() {
      return minimumLongitude;
    }

    public double getMaximumLatitude() {
      return maximumLatitude;
    }

    public double getMaximumLongitude() {
      return maximumLongitude;
    }
  }
}
