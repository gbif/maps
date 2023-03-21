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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.gbif.maps.TileServerConfiguration;
import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.bin.SquareBin;
import org.gbif.maps.common.filter.PointFeatureFilters;
import org.gbif.maps.common.filter.Range;
import org.gbif.maps.common.filter.VectorTileFilters;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.gbif.maps.resource.Params.BIN_MODE_HEX;
import static org.gbif.maps.resource.Params.BIN_MODE_SQUARE;
import static org.gbif.maps.resource.Params.DEFAULT_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.DEFAULT_SQUARE_SIZE;
import static org.gbif.maps.resource.Params.HEX_TILE_SIZE;
import static org.gbif.maps.resource.Params.SQUARE_TILE_SIZE;
import static org.gbif.maps.resource.Params.enableCORS;
import static org.gbif.maps.resource.Params.mapKeys;
import static org.gbif.maps.resource.Params.toMinMaxYear;

/**
 * The tile resource for the simple GBIF data layers (i.e. HBase sourced, preprocessed).
 */
@OpenAPIDefinition(
  info = @Info(
    title = "Maps API",
    version = "v2",
    // This huge chunk of Markdown/HTML isn't ideal, but I think it's important we can continue to show images
    // in the map API documentation.
    description =
      "The mapping API is a [web map tile service](https://www.ogc.org/standard/wmts/) making it straightforward to " +
        "visualize GBIF content on interactive maps, and overlay content from other sources.\n" +
        "\n" +
        "## Feature overview\n" +
        "\n" +
        "<div style='text-align: center'>\n" +
        "  <img src='https://api.gbif.org/v2/map/occurrence/density/2/3/2@1x.png?taxonKey=212&country=AU&style=orangeHeat.point' style='max-width: inherit; " +
        "background-image: url(https://tile.gbif.org/3857/omt/2/3/2@1x.png?style=gbif-dark); background-size: 256px 256px;' width='256' height='256'></a><br>\n" +
        "  Birds (<i>Aves</i>) in Australia.\n" +
        "</div>\n" +
        "\n" +
        "The following features are supported:\n" +
        "* Map layers are available for a **country**, **dataset**, **taxon** (including species, subspecies or higher " +
        "  taxa), **publisher**, **publishing country** or **network**.\n" +
        "  These layers can be filtered by year range, basis of record and country.\n" +
        "* Data is returned as points, or “binned” into hexagons or squares.\n" +
        "* Four map projections are supported.\n" +
        "* Tiles are available in vector format for client styling, or raster format with predefined styles.\n" +
        "* Arbitrary search terms are also supported, though binning is required for these searches.\n" +
        "\n" +
        "This service is intended for use with commonly used clients such as the  [OpenLayers](https://openlayers.org/) " +
        "or [Leaflet](https://leafletjs.com/) Javascript libraries, [Google Maps](https://developers.google.com/maps/), or " +
        "some GIS software including [QGIS](https://www.qgis.org/). These libraries allow the GBIF layers to be " +
        "visualized with other content, such as those coming from [web map service (WMS)](https://www.ogc.org/standard/wms/) " +
        "providers. It should be noted that the mapping API is not a WMS service, nor does it support WFS capabilities.\n" +
        "\n" +
        "## Tile formats\n" +
        "\n" +
        "Two tile formats are available: **vector tiles** and **raster tiles**. The raster tiles are generated from the " +
        "vector tiles.\n" +
        "\n" +
        "A modern web browser can show either format. The styling of vector tiles is determined by client-side " +
        "configuration (e.g. Javascript); raster tiles are styled according to a limited set of GBIF styles. *Point* " +
        "vector tiles are usually larger than an equivalent raster tile and can often be slow to render.  Simple vector " +
        "tiles (few points, or any number of squares or hexagons) are small, fast to render, and can make zooming in " +
        "and out smoother.\n" +
        "\n" +
        "Vector tiles use [Mapbox Vector Tile format](https://www.mapbox.com/vector-tiles/), and contain a single layer " +
        "`occurrence`. Objects in that layer are either points (default) or polygons (if chosen). Each object has a " +
        "`total` value; that is the number of occurrences at that point or in the polygon.\n" +
        "\n" +
        "Raster tiles are provided in PNG format, and are normally 512px wide squares.\n" +
        "\n" +
        "## Map styling\n" +
        "\n" +
        "Vector tiles must by styled by the client.\n" +
        "\n" +
        "Raster styles are predefined, and chosen with the `style=` parameter.  All available styles are shown below. " +
        "Note the styles labelled “approimate” are not intended for precise analysis.\n" +
        "\n" +
        "<table style='text-align: center; border-collapse: collapse;'>\n" +
        "  <tr>\n" +
        "    <th colspan='2'>Style</th>\n" +
        "    <th colspan='10'>Record count</th>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <th></th>\n" +
        "    <th></th>\n" +
        "    <td colspan='2' width='14%'>1–10</td>\n" +
        "    <td colspan='2' width='14%'>11–100</td>\n" +
        "    <td colspan='2' width='14%'>101–1000</td>\n" +
        "    <td colspan='2' width='14%'>1001–10000</td>\n" +
        "    <td width='14%'>10001–100000</td>\n" +
        "    <td width='14%'>100001+</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th rowspan='3'>Classic</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=classic.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='classic.point' width='128'><br>classic.point</td>\n" +
        "    <td colspan='2' rowspan='3' style='background-color:#FFFF00'></td>\n" +
        "    <td colspan='2' rowspan='3' style='background-color:#FFCC00'></td>\n" +
        "    <td colspan='2' rowspan='3' style='background-color:#FF9900'></td>\n" +
        "    <td colspan='2' rowspan='3' style='background-color:#FF6600'></td>\n" +
        "    <td colspan='2' rowspan='3' style='background-color:#D60A00'></td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=classic.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='classic.point' width='128'><br>classic.poly</td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=classic-noborder.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='classic-noborder.point' width='128'><br>classic-noborder.poly</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th rowspan='3'>Purple-yellow</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=purpleYellow.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='purpleYellow.point' width='128'><br>purpleYellow.point</td>\n" +
        "    <td rowspan='3' style='background-color:#5e0063'></td>\n" +
        "    <td rowspan='3' style='background-color:#851362'></td>\n" +
        "    <td rowspan='3' style='background-color:#a42e61'></td>\n" +
        "    <td rowspan='3' style='background-color:#be4c60'></td>\n" +
        "    <td rowspan='3' style='background-color:#d26b63'></td>\n" +
        "    <td rowspan='3' style='background-color:#e28b6b'></td>\n" +
        "    <td rowspan='3' style='background-color:#eeab79'></td>\n" +
        "    <td rowspan='3' style='background-color:#f7cb8e'></td>\n" +
        "    <td colspan='2' rowspan='3' style='background-color:#ffebaa'></td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=purpleYellow.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='purpleYellow.point' width='128'><br>purpleYellow.poly</td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=purpleYellow-noborder.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='purpleYellow-noborder.point' width='128'><br>purpleYellow-noborder.poly</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th rowspan='3'>Green</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=green.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='green.point' width='128'><br>green.point</td>\n" +
        "    <td rowspan='3' style='background-color:#4D7C40'></td>\n" +
        "    <td rowspan='3' style='background-color:#608A50'></td>\n" +
        "    <td rowspan='3' style='background-color:#70955E'></td>\n" +
        "    <td rowspan='3' style='background-color:#85A36F'></td>\n" +
        "    <td rowspan='3' style='background-color:#95AD7C'></td>\n" +
        "    <td rowspan='3' style='background-color:#AABA8D'></td>\n" +
        "    <td rowspan='3' style='background-color:#BAC599'></td>\n" +
        "    <td rowspan='3' style='background-color:#CCD0A8'></td>\n" +
        "    <td rowspan='3' style='background-color:#DFDCB7'></td>\n" +
        "    <td rowspan='3' style='background-color:#EFE6C4'></td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=green.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='green.point' width='128'><br>green.poly</td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=green-noborder.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='green.point' width='128'><br>green-noborder.poly</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th rowspan='4'>Heat</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=purpleHeat.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='purpleHeat.point' width='128'><br>purpleHeat.point</td>\n" +
        "    <td colspan='10' style='background: linear-gradient(to right, #ff21ad, #ffacff);'>Approximate</td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=blueHeat.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='blueHeat.point' width='128'><br>blueHeat.point</td>\n" +
        "    <td colspan='10' style='background: linear-gradient(to right, #206EFF, #a9fcff);'>Approximate</td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=orangeHeat.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='orangeHeat.point' width='128'><br>orangeHeat.point</td>\n" +
        "    <td colspan='10' style='background: linear-gradient(to right, #C06719, #fffb90);'>Approximate</td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=greenHeat.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='greenHeat.point' width='128'><br>greenHeat.point</td>\n" +
        "    <td colspan='10' style='background: linear-gradient(to right, #369617, #daff88);'>Approximate</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th>Fire</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=fire.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='fire.point' width='128'><br>fire.point</td>\n" +
        "    <td colspan='10' style='background: linear-gradient(to right, #5E0700, #EF4712, #DC6902, #F09C00, #F2F7F0)'>Approximate</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th>Glacier</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?style=glacier.point&amp;srs=EPSG:4326&amp;taxonKey=797' title='glacier.point' width='128'><br>glacier.point</td>\n" +
        "    <td colspan='10' style='background: linear-gradient(to right, #045a8d, #2b8cbe, #74a9cf, #bdc9e1, #f1eef6)'>Approximate</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th rowspan='2'>Green 2</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=green2.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='green2.point' width='128'><br>green2.poly</td>\n" +
        "    <td colspan='2' rowspan='2' style='background-color:#edf8e9'></td>\n" +
        "    <td colspan='2' rowspan='2' style='background-color:#bae4b3'></td>\n" +
        "    <td colspan='2' rowspan='2' style='background-color:#74c476'></td>\n" +
        "    <td colspan='2' rowspan='2' style='background-color:#31a354'></td>\n" +
        "    <td colspan='2' rowspan='2' style='background-color:#006d2c'></td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=green2-noborder.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='green2.point' width='128'><br>green2-noborder.poly</td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th>iNaturalist</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=iNaturalist.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='iNaturalist.point' width='128'><br>iNaturalist.poly</td>\n" +
        "    <td colspan='2' style='background-color:#F7005A; opacity 0.9'></td>\n" +
        "    <td colspan='2' style='background-color:#D50067; opacity 0.9'></td>\n" +
        "    <td colspan='2' style='background-color:#B5006C; opacity 0.9'></td>\n" +
        "    <td colspan='2' style='background-color:#94006A; opacity 0.9'></td>\n" +
        "    <td style='background-color:#72005F; opacity 0.9'></td>\n" +
        "    <td style='background-color:#52034E; opacity 0.9'></td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th>Purple-white</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=purpleWhite.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='purpleWhite.point' width='128'><br>purpleWhite.poly</td>\n" +
        "    <td style='background-color:#a000a0'></td>\n" +
        "    <td style='background-color:#ab1fab'></td>\n" +
        "    <td style='background-color:#b73fb7'></td>\n" +
        "    <td style='background-color:#c35fc3'></td>\n" +
        "    <td style='background-color:#cf7fcf'></td>\n" +
        "    <td style='background-color:#db9fdb'></td>\n" +
        "    <td style='background-color:#e7bfe7'></td>\n" +
        "    <td style='background-color:#f3dff3'></td>\n" +
        "    <td colspan='2' style='background-color:#ffffff'></td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th>Red</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=red.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='red.point' width='128'><br>red.poly</td>\n" +
        "    <td colspan='10' style='background-color:#cc0000'></td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th rowspan='3'>Marker</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=blue.marker&amp;srs=EPSG:4326&amp;taxonKey=797' title='blue.marker' width='128'><br>blue.marker</td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #209fffa7  4px, white  4px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #209fffa7  8px, white  8px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #209fffa7 12px, white 12px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #209fffa7 14px, white 14px);'></td>\n" +
        "    <td style='background: radial-gradient(circle, #209fffa7 16px, white 16px);'></td>\n" +
        "    <td style='background: radial-gradient(circle, #209fffa7 25px, white 25px);'></td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=orange.marker&amp;srs=EPSG:4326&amp;taxonKey=797' title='orange.marker' width='128'><br>orange.marker</td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #FF6347CD  4px, white  4px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #FF6347CD  8px, white  8px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #FF6347CD 12px, white 12px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #FF6347CD 14px, white 14px);'></td>\n" +
        "    <td style='background: radial-gradient(circle, #FF6347CD 16px, white 16px);'></td>\n" +
        "    <td style='background: radial-gradient(circle, #FF6347CD 25px, white 25px);'></td>\n" +
        "  </tr>\n" +
        "  <tr>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@Hx.png?bin=hex&amp;hexPerTile=20&amp;style=outline.poly&amp;srs=EPSG:4326&amp;taxonKey=797' title='outline.poly' width='128'><br>outline.poly</td>\n" +
        "    <td colspan='10' style='background: radial-gradient(circle, white 15px, #7b7b7bcd 16px, white 17px);'></td>\n" +
        "  </tr>\n" +
        "\n" +
        "  <tr>\n" +
        "    <th rowspan='3'>Geo-centroid</th>\n" +
        "    <td><img src='https://api.gbif.org/v2/map/occurrence/adhoc/0/0/0@Hx.png?mode=GEO_CENTROID&amp;style=scaled.circles&amp;srs=EPSG:4326&amp;taxonKey=797' title='scaled.circles' width='128'><br>scaled.circles</td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #fed976ff  6px, white  6px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #fd8d3ccd  7px, white  7px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #fd8d3cb3 10px, white 10px);'></td>\n" +
        "    <td colspan='2' style='background: radial-gradient(circle, #f03b2099 16px, white 16px);'></td>\n" +
        "    <td colspan='3' style='background: radial-gradient(circle, #bd002699 30px, white 30px);'></td>\n" +
        "  </tr>\n" +
        "</table>\n" +
        "\n" +
        "## Example queries\n" +
        "\n" +
        "| Description | Sample |\n" +
        "|-------------|--------|\n" +
        "" +
        "| All occurrences — no additional parameters <br/> " +
        "  `https://api.gbif.org/v2/map/occurrence/density/{z}/{x}/{y}@1x.png?style=purpleYellow.point` " +
        "| <a href='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?style=purpleYellow.point'><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?style=purpleYellow.point' width='64' height='64' style='max-width: inherit; background-image: url(https://tile.gbif.org/3857/omt/0/0/0@1x.png?style=gbif-light); background-size: 64px 64px;'/></a>\n" +
        "" +
        "| All birds (*Aves*) by small hexagons <br/> " +
        "  `https://api.gbif.org/v2/map/occurrence/density/{z}/{x}/{y}@1x.png?taxonKey=212&bin=hex&hexPerTile=30&style=classic-noborder.poly` " +
        "| <a href='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?taxonKey=212&bin=hex&hexPerTile=30&style=classic-noborder.poly'><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?taxonKey=212&bin=hex&hexPerTile=30&style=classic-noborder.poly' width='64' height='64' style='max-width: inherit; background-image: url(https://tile.gbif.org/3857/omt/0/0/0@1x.png?style=gbif-light); background-size: 64px 64px;'/></a>\n" +
        "" +
        "| All birds observed by machine between 2015 and 2017 as squares <br/> " +
        "  `https://api.gbif.org/v2/map/occurrence/density/{z}/{x}/{y}@1x.png?taxonKey=212&basisOfRecord=MACHINE_OBSERVATION&years=2015,2017&bin=square&squareSize=128&style=purpleYellow-noborder.poly` " +
        "| <a href='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?taxonKey=212&basisOfRecord=MACHINE_OBSERVATION&years=2015,2017&bin=square&squareSize=128&style=purpleYellow-noborder.poly'><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?taxonKey=212&basisOfRecord=MACHINE_OBSERVATION&years=2015,2017&bin=square&squareSize=128&style=purpleYellow-noborder.poly' width='64' height='64' style='max-width: inherit; background-image: url(https://tile.gbif.org/3857/omt/0/0/0@1x.png?style=gbif-light); background-size: 64px 64px;'/></a>\n" +
        "" +
        "| All preserved, fossil or living specimens from before 1900 published by Swedish publishers, in Arctic projection <br/> " +
        "  `https://api.gbif.org/v2/map/occurrence/density/{z}/{x}/{y}@1x.png?srs=EPSG:3575&publishingCountry=SE&basisOfRecord=PRESERVED_SPECIMEN&basisOfRecord=FOSSIL_SPECIMEN&basisOfRecord=LIVING_SPECIMEN&year=1600,1899&bin=square&squareSize=128&style=green.poly` " +
        "| <a href='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?srs=EPSG:3575&publishingCountry=SE&basisOfRecord=PRESERVED_SPECIMEN&basisOfRecord=FOSSIL_SPECIMEN&basisOfRecord=LIVING_SPECIMEN&year=1600,1899&bin=square&squareSize=128&style=green.poly'><img src='https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?srs=EPSG:3575&publishingCountry=SE&basisOfRecord=PRESERVED_SPECIMEN&basisOfRecord=FOSSIL_SPECIMEN&basisOfRecord=LIVING_SPECIMEN&year=1600,1899&bin=square&squareSize=128&style=green.poly' width='64' height='64' style='max-width: inherit; background-image: url(https://tile.gbif.org/3575/omt/0/0/0@1x.png?style=gbif-light); background-size: 64px 64px;'/></a>\n" +
        "" +
        "| Ad-hoc query for occurrences with images, observed in January <br/> " +
        "  `https://api.gbif.org/v2/map/occurrence/adhoc/{z}/{x}/{y}@1x.png?srs=EPSG:4326&style=classic.poly&bin=square&squareSize=128&mediaType=StillImage&month=1` " +
        "| <a href='https://api.gbif.org/v2/map/occurrence/adhoc/0/0/0@1x.png?srs=EPSG:4326&style=classic.poly&bin=square&squareSize=128&mediaType=StillImage&month=1'><img src='https://api.gbif.org/v2/map/occurrence/adhoc/0/0/0@1x.png?srs=EPSG:4326&style=classic.poly&bin=square&squareSize=128&mediaType=StillImage&month=1' width='64' height='64' style='max-width: inherit; background-image: url(https://tile.gbif.org/4326/omt/0/0/0@1x.png?style=gbif-light); background-size: 64px 64px;'/></a>\n" +
        "\n" +
        "<iframe height='265' style='width:100%; height:500px' scrolling='no' title='Taxon overlay' src='https://codepen.io/hofft/embed/GRROjmo?height=265&theme-id=0&default-tab=js,result'>\n" +
        "  See the Pen <a href='https://codepen.io/hofft/pen/GRROjmo'>Taxon overlay</a> by Morten Hofft\n" +
        "  (<a href='https://codepen.io/hofft'>@hofft</a>) on <a href='https://codepen.io'>CodePen</a>.\n" +
        "</iframe>\n" +
        "\n" +
        "## Projections\n" +
        "\n" +
        "The projection defines how coordinates on Earth are transformed to a two-dimensional surface.\n\n" +
        "The tile schema defines how that two dimensional surface is split into smaller square images, and how those images are addressed.\n\n" +
        "Up to four projections are available, depending on the endpoint.  Information on the projections is available on the [GBIF base map tiles](https://tile.gbif.org/ui/) page.\n" +
        "\n" +
        "## Base map tiles\n" +
        "\n" +
        "Base map tiles showing land and oceans, forests, roads and so on are available at [tile.gbif.org](https://tile.gbif.org/) " +
        "for users of this API.\n" +
        "\n" +
        "## Further resources\n" +
        "\n" +
        "* [Base map examples](https://tile.gbif.org/ui/)\n" +
        "* [Raster style demos](https://api.gbif.org/v2/map/demo.html)\n" +
        "* [OpenLayers-based toolbox](https://api.gbif.org/v2/map/debug/ol/)\n",
    termsOfService = "https://www.gbif.org/terms"),
  servers = {
    @Server(url = "https://api.gbif.org/v2/", description = "Production"),
    @Server(url = "https://api.gbif-uat.org/v2/", description = "User testing")
  })
@Tag(name = "Occurrence maps",
  description = "This API provides pre-calculated and ad-hoc occurrence map tiles.",
  extensions = @io.swagger.v3.oas.annotations.extensions.Extension(
    name = "Order", properties = @ExtensionProperty(name = "Order", value = "0100"))
)
@RestController
@RequestMapping(
  value = "/occurrence/density"
)
@Profile("!es-only")
public final class TileResource {

  private static final Logger LOG = LoggerFactory.getLogger(TileResource.class);

  // VectorTile layer name for the composite layer produced when merging basis of record
  // layers together
  private static final String LAYER_OCCURRENCE = "occurrence";

  private static final VectorTileDecoder DECODER = new VectorTileDecoder();

  // extents of the WGS84 Plate Careé Zoom 0 tiles
  static final Double2D ZOOM_0_WEST_NW = new Double2D(-180, 90);
  static final Double2D ZOOM_0_WEST_SE = new Double2D(0, -90);
  static final Double2D ZOOM_0_EAST_NW = new Double2D(0, 90);
  static final Double2D ZOOM_0_EAST_SE = new Double2D(180, -90);

  static {
    DECODER.setAutoScale(false); // important to avoid auto scaling to 256 tiles
  }

  private final HBaseMaps hbaseMaps;
  private final int tileSize;
  private final int bufferSize;

  /**
   * Construct the resource
   * @param hbaseMaps The data layer to the maps
   * @param configuration service configuration
   */
  @Autowired
  public TileResource(HBaseMaps hbaseMaps, TileServerConfiguration configuration) {
    this.hbaseMaps = hbaseMaps;
    this.tileSize = configuration.getHbase().getTileSize();
    this.bufferSize = configuration.getHbase().getBufferSize();
  }

  @Operation(
    operationId = "getDensityTile",
    summary = "Precalculated density tile",
    description = "Retrieves a tile showing occurrence locations in [Mapbox Vector Tile format](https://www.mapbox.com/vector-tiles/)\n" +
      "\n" +
      "Tiles contain a single layer `occurrence`. Features in that layer are either points (default) or polygons " +
      "(if chosen). Each feature has a `total` value; that is the number of occurrences at that point or in the polygon.\n" +
      "\n" +
      "**One primary search parameter is permitted**, from these: `taxonKey`, `datasetKey`, `country`, `networkKey`, " +
      "`publishingOrg`, `publishingCountry`.\n" +
      "\n" +
      "This can be combined with the parameter `country`, this limits the primary search to occurrences in that country.\n" +
      "\n"
    )
  @Parameters(
    value = {
      @Parameter(
        name = "verbose",
        description = "If set, counts will be grouped by year to allow a fast view of different years. " +
          "If unset (the default), the total will be a count for all years.",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY
      )
    }
  )
  @CommonOpenAPI.TileProjectionAndStyleParameters
  @CommonOpenAPI.BinningParameters
  @CommonOpenAPI.DensitySearchParameters
  @CommonOpenAPI.TileResponses
  @RequestMapping(
    method = RequestMethod.GET,
    value = "/{z}/{x}/{y}.mvt",
    produces = "application/x-protobuf"
  )
  @Timed
  public byte[] all(
    @PathVariable("z") int z,
    @PathVariable("x") long x,
    @PathVariable("y") long y,
    @RequestParam(value = "srs", defaultValue = "EPSG:3857") String srs,  // default as SphericalMercator
    @RequestParam(value = "basisOfRecord", required = false) List<String> basisOfRecord,
    @RequestParam(value = "year", required = false) String year,
    @RequestParam(value = "verbose", defaultValue = "false") boolean verbose,
    @RequestParam(value = "bin", required = false) String bin,
    @RequestParam(value = "hexPerTile", defaultValue = DEFAULT_HEX_PER_TILE) int hexPerTile,
    @RequestParam(value = "squareSize", defaultValue = DEFAULT_SQUARE_SIZE) int squareSize,
    HttpServletResponse response,
    HttpServletRequest request
    ) throws Exception {

    enableCORS(response);
    String[] mapKeys = mapKeys(request);
    DatedVectorTile datedVectorTile = getTile(z,x,y,mapKeys[0],mapKeys[1],srs,basisOfRecord,year,verbose,bin,hexPerTile,squareSize);

    if (datedVectorTile.date != null) {
      // A weak ETag is set, as tiles may not be byte-for-byte identical after filtering and binning.
      response.setHeader("ETag", String.format("W/\"%s\"", datedVectorTile.date));
    }
    return datedVectorTile.tile;
  }

  /**
   * Returns a capabilities response with the extent and year range built by inspecting the zoom 0 tiles of the
   * EPSG:4326 projection.
   */
  @Operation(
    operationId = "getDensityCapabilities",
    summary = "Summary of a density tile map query",
    description = "A summary of the data available for a [density tile](#operation/getDensityTile) query.\n" +
      "\n" +
      "It accepts the same search parameters as the density tile query."
  )
  @CommonOpenAPI.DensitySearchParameters
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Map capabilities details."),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid search request.",
        content = @Content()
      )
    }
  )
  @RequestMapping(
    method = RequestMethod.GET,
    value = "capabilities.json",
    produces = MediaType.APPLICATION_JSON_VALUE
  )
  @Timed
  public Capabilities capabilities(HttpServletResponse response, HttpServletRequest request)
    throws Exception {

    enableCORS(response);
    String[] mapKey = mapKeys(request);

    Capabilities.CapabilitiesBuilder builder = Capabilities.CapabilitiesBuilder.newBuilder();
    DatedVectorTile west = getTile(0,0,0,mapKey[0],mapKey[1],"EPSG:4326",null,null,true,null,0,0);
    DatedVectorTile east = getTile(0,1,0,mapKey[0],mapKey[1],"EPSG:4326",null,null,true,null,0,0);
    builder.collect(west.tile, ZOOM_0_WEST_NW, ZOOM_0_WEST_SE, west.date);
    builder.collect(east.tile, ZOOM_0_EAST_NW, ZOOM_0_EAST_SE, east.date);
    Capabilities capabilities = builder.build();
    LOG.info("Capabilities: {}", capabilities);

    if (capabilities.getGenerated() != null) {
      response.setHeader("ETag", String.format("\"%s\"", capabilities.getGenerated()));
    }

    return capabilities;
  }

  DatedVectorTile getTile(
    int z,
    long x,
    long y,
    String mapKey,
    String countryMaskKey,
    String srs,
    List<String> basisOfRecord,
    String year,
    boolean verbose,
    String bin,
    int hexPerTile,
    int squareSize
  ) throws Exception {
    Preconditions.checkArgument(bin == null || BIN_MODE_HEX.equalsIgnoreCase(bin) || BIN_MODE_SQUARE.equalsIgnoreCase(bin),
                                "Unsupported bin mode");
    LOG.info("MapKey: {} with mask {}", mapKey, countryMaskKey);

    Range years = toMinMaxYear(year);
    Set<String> bors = basisOfRecord == null || basisOfRecord.isEmpty() ? null : Sets.newHashSet(basisOfRecord);

    DatedVectorTile datedVectorTile = filteredVectorTile(z, x, y, mapKey, srs, bors, years, verbose);

    // If we have a country mask, retrieve the tile and apply the mask.
    if (countryMaskKey != null) {
      VectorTileEncoder encoder = new VectorTileEncoder(tileSize, tileSize/4 /* width of a hex? */, false);

      byte[] countryMaskVectorTile = filteredVectorTile(z, x, y, countryMaskKey, srs, bors, years, false).tile;

      VectorTileFilters.maskTileByTile(encoder, LAYER_OCCURRENCE, datedVectorTile.tile, countryMaskVectorTile);
      datedVectorTile.tile = encoder.encode();
    }

    // depending on the query, direct the request
    if (bin == null) {
      return datedVectorTile;

    } else if (BIN_MODE_HEX.equalsIgnoreCase(bin)) {
      HexBin binner = new HexBin(HEX_TILE_SIZE, hexPerTile);
      try {
        return new DatedVectorTile(binner.bin(datedVectorTile.tile, z, x, y), datedVectorTile.date);
      } catch (IllegalArgumentException e) {
        // happens on empty tiles
        return datedVectorTile;
      }

    } else if (BIN_MODE_SQUARE.equalsIgnoreCase(bin)) {
      SquareBin binner = new SquareBin(SQUARE_TILE_SIZE, squareSize);
      try {
        return new DatedVectorTile(binner.bin(datedVectorTile.tile, z, x, y), datedVectorTile.date);
      } catch (IllegalArgumentException e) {
        // happens on empty tiles
        return datedVectorTile;
      }

    } else {
      throw new IllegalArgumentException("Unsupported bin mode: " + bin); // cannot happen due to conditional check above
    }
  }

  /**
   * Retrieves the data from HBase, applies the filters and merges the result into a vector tile containing a single
   * layer named {@link TileResource#LAYER_OCCURRENCE}.
   * <p>
   * This method handles both pre-tiled and simple feature list stored data, returning a consistent format of vector
   * tile regardless of the storage format.  Please note that the tile size can vary and should be inspected before use.
   *
   * @param z The zoom level
   * @param x The tile X address for the vector tile
   * @param y The tile Y address for the vector tile
   * @param mapKey The map key being requested
   * @param srs The SRS of the requested tile
   * @param basisOfRecords To include in the filter.  An empty or null value will include all values
   * @param years The year range to filter.
   * @return A byte array representing an encoded vector tile.  The tile may be empty.
   * @throws IOException Only if the data in HBase is corrupt and cannot be decoded.  This is fatal.
   */
  private DatedVectorTile filteredVectorTile(int z, long x, long y, String mapKey, String srs,
                                    @Nullable Set<String> basisOfRecords, @NotNull Range years, boolean verbose)
    throws IOException {

    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

    // Attempt to get a preprepared tile first, before falling back to a point tile
    Optional<byte[]> encoded = hbaseMaps.getTile(mapKey, srs, z, x, y);
    String date;

    if (encoded.isPresent()) {
      date = hbaseMaps.getTileDate().orElse(null);
      LOG.info("Found tile {} {}/{}/{} for key {} with encoded length of {} and date {}", srs, z, x, y, mapKey, encoded.get().length, date);

      VectorTileFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, encoded.get(),
                                            years, basisOfRecords, verbose);
      return new DatedVectorTile(encoder.encode(), date);
    } else {
      // The tile size is chosen to match the size of preprepared tiles.
      date = hbaseMaps.getPointsDate().orElse(null);
      Optional<PointFeature.PointFeatures> optionalFeatures = hbaseMaps.getPoints(mapKey);
      if (optionalFeatures.isPresent()) {
        TileProjection projection = Tiles.fromEPSG(srs, tileSize);
        PointFeature.PointFeatures features = optionalFeatures.get();
        LOG.info("Found {} features for key {}, date {}", features.getFeaturesCount(), mapKey, date);

        PointFeatureFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, features.getFeaturesList(),
                                                projection, TileSchema.fromSRS(srs), z, x, y, tileSize, bufferSize,
                                                years, basisOfRecords);
      }
      return new DatedVectorTile(encoder.encode(), date); // may be empty
    }
  }
}
