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

    /*
     * Only the map key is taken into account for the V2 capabilities.
     *
     * This code isn't tested, but it could work.  See mapnik-server for a working Javascript version.
     *
    Set<String> basisOfRecord = new HashSet();

    // Year ranges.  We assume ranges given are continuous, but don't permit showing no-year records as well as a range —
    // unless the whole range (pre-1900 to 2020) is selected, then no year filter is requested.
    int
        obsStart = 9999,
        obsEnd = -1,
        spStart = 9999,
        spEnd = -1,
        othStart = 9999,
        othEnd = -1;
    boolean noYear = false;
    boolean obs = false, sp = false, oth = false;

    for (String l : layers) {
      if (l == "LIVING") {
        basisOfRecord.add("LIVING_SPECIMEN");
      } else if (l == "FOSSIL") {
        basisOfRecord.add("FOSSIL_SPECIMEN");
      } else {
        int first_ = l.indexOf('_');
        int second_ = l.indexOf('_', first_+1);

        String prefix = l.substring(0, first_);
        String startYear = l.substring(first_+1, second_);
        String endYear = l.substring(second_+1);

        if ("NO".equals(startYear)) {
          noYear = true;
        } else if ("PRE".equals(startYear)) {
          startYear = "0";
        }

        switch (prefix) {
          case "OBS":
            basisOfRecord.add("OBSERVATION");
            basisOfRecord.add("HUMAN_OBSERVATION");
            basisOfRecord.add("MACHINE_OBSERVATION");
            if (startYear != "NO") {
              obsStart = Math.min(obsStart, Integer.parseInt(startYear));
              obsEnd = Math.max(obsEnd, Integer.parseInt(endYear));
            }
            obs = true;
            break;
          case "SP":
            basisOfRecord.add("PRESERVED_SPECIMEN");
            if (startYear != "NO") {
              spStart = Math.min(spStart, Integer.parseInt(startYear));
              spEnd = Math.max(spEnd, Integer.parseInt(endYear));
            }
            sp = true;
            break;
          case "OTH":
            basisOfRecord.add("MATERIAL_SAMPLE");
            basisOfRecord.add("LITERATURE");
            basisOfRecord.add("UNKNOWN");
            if (startYear != "NO") {
              othStart = Math.min(othStart, Integer.parseInt(startYear));
              othEnd = Math.max(othEnd, Integer.parseInt(endYear));
            }
            oth = true;
            break;
          default:
        }
      }
    }

    // If all are selected, don't filter.
    if (basisOfRecord.size() == 9) {
      basisOfRecord.clear();
    }

    String year;

    // All year filters must apply to all record types.
    boolean yearsMismatch = false;
    yearsMismatch &= obs && sp  && (obsStart !=  spStart || obsEnd !=  spEnd);
    yearsMismatch &= obs && oth && (obsStart != othStart || obsEnd != othEnd);
    yearsMismatch &=  sp && oth && ( spStart != othStart ||  spEnd != othEnd);

    if (!yearsMismatch) {
      if (obs && obsStart == 9999 || sp && spStart == 9999 || oth && othStart == 9999) {
        year = null;
      } else if (obs) {
        year = obsStart + "," + obsEnd;
      } else if (sp) {
        year = spStart + "," + spEnd;
      } else if (oth) {
        year = othStart + "," + othEnd;
      } else {
        // Only fossils and/or living
        year = null;
      }
    } else {
      String detail = "OBS "+obsStart+"-"+obsEnd+"; "+
          "SP "+spStart+"-"+spEnd+"; "+
          "OTH "+othStart+"-"+othEnd+"\n";

      throw new RuntimeException("Start and end years must be the same for each layer (BasisOfRecord): "+detail);
    }

    if (year == "0,2020" && noYear) {
      year = null;
    } else if (year != null && noYear) {
      throw new RuntimeException("Can't display undated records as well as a range of dated ones.\n");
    }
     */

    Capabilities.CapabilitiesBuilder builder = Capabilities.CapabilitiesBuilder.newBuilder();
    DatedVectorTile west = tileResource.getTile(0,0,0,mapKey,"EPSG:4326",null,null,true,null,0,0);
    DatedVectorTile east = tileResource.getTile(0,1,0,mapKey,"EPSG:4326",null,null,true,null,0,0);
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
      this.minimumLatitude = c.getMinLat();
      this.minimumLongitude = c.getMinLng();
      this.maximumLatitude = c.getMaxLat();
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
