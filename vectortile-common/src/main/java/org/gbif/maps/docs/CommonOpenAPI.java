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
package org.gbif.maps.docs;

import static java.lang.annotation.ElementType.METHOD;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.Explode;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.UUID;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;

public class CommonOpenAPI {

  // These parameters are used for the density tile and capabilities request.
  @Target({METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
    value = {
      @Parameter(
        name = "z",
        description = "Zoom level",
        schema = @Schema(minimum = "0", maximum = "16"),
        in = ParameterIn.PATH,
        required = true
      ),
      @Parameter(
        name = "x",
        description = "Tile map column. 0 is the leftmost column, at the rightmost column `x == 2ᶻ – 1`, " +
          "except for EPSG:4326 projection where `x == 2ᶻ⁺¹ – 1`",
        schema = @Schema(minimum = "0", maximum = "2ᶻ⁺¹ – 1"),
        in = ParameterIn.PATH,
        required = true
      ),
      @Parameter(
        name = "y",
        description = "Tile map row. 0 is the top row, for the bottom row `y == 2ᶻ – 1`.",
        schema = @Schema(minimum = "0", maximum = "2ᶻ – 1"),
        in = ParameterIn.PATH,
        required = true
      ),
      @Parameter(
        name = "format",
        description = "Map tile format and resolution.\n" +
          "* `.mvt` for a vector tile\n" +
          "* `@Hx.png` for a 256px raster tile (for legacy clients)\n" +
          "* `@1x.png` for a 512px raster tile\n" +
          "* `@2x.png` for a 1024px raster tile\n" +
          "* `@3x.png` for a 2048px raster tile\n" +
          "* `@4x.png` for a 4096px raster tile\n" +
          "The larger raster tiles are intended for high resolution displays, i.e. 4k monitors and many mobile phones.",
        schema = @Schema(allowableValues = { ".mvt", "@Hx.png", "@1x.png", "@2x.png", "@3x.png", "@4x.png" }),
        in = ParameterIn.PATH,
        required = true
      ),
      @Parameter(
        name = "srs",
        description = "Map projection. One of\n" +
          "* `EPSG:3857` (Web Mercator)\n" +
          "* `EPSG:4326` (WGS84 plate careé)\n" +
          "* `EPSG:3575` (Arctic LAEA)\n" +
          "* `EPSG:3031` (Antarctic stereographic)\n" +
          "See [Projections](#projections)",
        schema = @Schema(allowableValues = { "EPSG:3857", "EPSG:4326", "EPSG:3575", "EPSG:3031" }),
        in = ParameterIn.QUERY,
        required = true
      ),
      @Parameter(
        name = "style",
        description = "Raster map style — only applies to raster (PNG) tiles.\n" +
          "\n" +
          "Choose from one of the available styles, the default is `classic.point`.\n" +
          "\n" +
          "Styles ending with `.point` should only be used by non-binned tiles.  Styles ending with `.poly` or " +
          "`.marker.` should be used with hexagonal- or square-binned tiles.\n" +
          "\n" +
          "See [map styles](#section/Map-styling).",
        schema = @Schema(allowableValues = {
          "purpleHeat.point",
          "blueHeat.point",
          "orangeHeat.point",
          "greenHeat.point",
          "classic.point",
          "purpleYellow.point",
          "green.point",
          "fire.point",
          "glacier.point",
          "classic.poly",
          "classic-noborder.poly",
          "purpleYellow.poly",
          "purpleYellow-noborder.poly",
          "green.poly",
          "green-noborder.poly",
          "green2.poly",
          "iNaturalist.poly",
          "purpleWhite.poly",
          "red.poly",
          "blue.marker",
          "orange.marker",
          "outline.poly",
          "scaled.circles"
        }),
        in = ParameterIn.QUERY
      )
    }
  )
  public @interface TileProjectionAndStyleParameters {}

  @Target({METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
    value = {
      @Parameter(
        name = "bin",
        description = "Binning style.  `hex` will aggregate points into a hexagonal grid (see `hexPerTile`). " +
          "`square` will aggregate into a square grid (see `squareSize`).",
        schema = @Schema(allowableValues = { "hex", "square" }),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "hexPerTile",
        description = "With `bin=hex`, sets the number of hexagons horizontally across a tile.",
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "squareSize",
        description = "With `bin=square`, sets the size of the squares in pixels on a 4096px tile.  Choose a factor " +
          "of 4096 so they tessalete correctly.",
        schema = @Schema(allowableValues = { "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096" }),
        in = ParameterIn.QUERY
      )
    }
  )
  public @interface BinningParameters {};

  // These parameters are used for the density tile and capabilities request.
  @Target({METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
    value = {
      @Parameter(
        name = "basisOfRecord",
        description = "Basis of record, as defined in our BasisOfRecord vocabulary.\n\n" +
          "The parameter may be repeated to retrieve occurrences with any of the chosen bases of record.",
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = BasisOfRecord.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "country",
        description = "The 2-letter country code (as per ISO-3166-1) of the country in which the occurrence " +
          "was recorded.",
        schema = @Schema(implementation = Country.class),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "taxonKey",
        description = "A taxon key from the GBIF backbone.",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "datasetKey",
        description = "The occurrence dataset key (a UUID).",
        schema = @Schema(implementation = UUID.class),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "publishingOrg",
        description = "The publishing organization's GBIF key (a UUID).",
        schema = @Schema(implementation = UUID.class),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "publishingCountry",
        description = "The 2-letter country code (as per ISO-3166-1) of the owning organization's country.",
        schema = @Schema(implementation = Country.class),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "networkKey",
        description = "The network's GBIF key (a UUID).",
        schema = @Schema(implementation = UUID.class),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "year",
        description = "The 4 digit year or year range. A year of 98 will be interpreted as 98 AD.\n\n" +
          "Ranges are written as `1990,2000`, `1990,` or `,1900`.",
        in = ParameterIn.QUERY
      )
    }
  )
  public @interface DensitySearchParameters {}

  // Standard vector and raster tile responses
  @Target({METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Map tile.",
        content = {
          @Content(mediaType = "application/x-protobuf", examples = @ExampleObject(externalValue = "https://api.gbif.org/v2/map/occurrence/density/2/3/2.mvn?taxonKey=212&country=AU")),
          @Content(mediaType = "image/png", examples = @ExampleObject(externalValue = "https://api.gbif.org/v2/map/occurrence/density/2/3/2@1x.png?taxonKey=212&country=AU&style=orangeHeat.point"))
        }),
      @ApiResponse(
        responseCode = "204",
        description = "Empty map tile.",
        content = @Content()
      ),
      @ApiResponse(
        responseCode = "400",
        description = "Incorrect request.",
        content = @Content()
      )
    }
  )
  public @interface TileResponses {}

}
