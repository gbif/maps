package org.gbif.maps.resource;

import java.util.List;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.*;
import static org.gbif.maps.resource.TileResource.*;

/**
 * Provide backward compatibility for the /v1/map/density/tile.json API call.
 *
 * Ignore layer and year handling, the logs suggest this isn't used, although the V1 tile-server does support it.
 * (Code possibly implementing it anyway was removed on 2017-08-22.)
 */
@Path("density")
@Singleton
public class BackwardCompatibility {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardCompatibility.class);

  private final TileResource tileResource;

  /**
   * Construct the resource
   */
  public BackwardCompatibility(TileResource tileResource) throws Exception {
    this.tileResource = tileResource;
  }

  /**
   * Returns a capabilities response with the extent and year range built by inspecting the zoom 0 tiles of the
   * EPSG:4326 projection.
   */
  @GET
  @Path("/tile.json")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public V1TileJson tileJson(
      @QueryParam("type") String type,
      @QueryParam("key") String key,
      @QueryParam("layers") List<String> layers,
      @Context HttpServletResponse response)
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
      this.minimumLatitude = Math.max(-85.0511, c.getMinLat());
      this.maximumLatitude = Math.min( 85.0511, c.getMaxLat());
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
