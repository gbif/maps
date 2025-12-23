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

public class OpenAPIDocs {

  // These parameters are used for the density tile and capabilities request.
  @Target({METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
    value = {
      @Parameter(
        name = "eventId",
        description = "Event ID of the record",
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "country",
        description = "The 2-letter country code (as per ISO-3166-1) of the country in which the event " +
          "was recorded.",
        schema = @Schema(implementation = Country.class),
        in = ParameterIn.QUERY
      ),
      @Parameter(
        name = "datasetKey",
        description = "The sampling event dataset key (a UUID).",
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
          @Content(mediaType = "application/x-protobuf", examples = @ExampleObject(externalValue = "https://api.gbif.org/v2/map/event/density/2/3/2.mvn?taxonKey=212&country=AU")),
          @Content(mediaType = "image/png", examples = @ExampleObject(externalValue = "https://api.gbif.org/v2/map/event/density/2/3/2@1x.png?taxonKey=212&country=AU&style=orangeHeat.point"))
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
