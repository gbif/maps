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

import org.gbif.maps.common.projection.SphericalMercator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A container object to produce JSON in TileJson format.
 * @see https://github.com/mapbox/tilejson-spec
 */
@JsonInclude(JsonInclude.Include.ALWAYS)
public class TileJson {
  private String attribution;
  private double[] bounds;
  private int[] center = new int[]{0,0,0};
  private String description;
  private String filesize;
  private String format = "pbf";
  private String id;
  private int maskLevel = 22;
  private int maxzoom = 22;
  private int minzoom = 0;
  private String name;
  @JsonProperty("private")
  private boolean private_;
  private String scheme = "xyx";
  private String tilejson = "2.0.0";
  private String[] tiles;
  @JsonProperty("vector_layers")
  private VectorLayer[] vectorLayers;


  public String getAttribution() {
    return attribution;
  }

  public void setAttribution(String attribution) {
    this.attribution = attribution;
  }

  public double[] getBounds() {
    return bounds;
  }

  public void setBounds(double[] bounds) {
    this.bounds = bounds;
  }

  public int[] getCenter() {
    return center;
  }

  public void setCenter(int[] center) {
    this.center = center;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getFilesize() {
    return filesize;
  }

  public void setFilesize(String filesize) {
    this.filesize = filesize;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public int getMaskLevel() {
    return maskLevel;
  }

  public void setMaskLevel(int maskLevel) {
    this.maskLevel = maskLevel;
  }

  public int getMaxzoom() {
    return maxzoom;
  }

  public void setMaxzoom(int maxzoom) {
    this.maxzoom = maxzoom;
  }

  public int getMinzoom() {
    return minzoom;
  }

  public void setMinzoom(int minzoom) {
    this.minzoom = minzoom;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isPrivate_() {
    return private_;
  }

  public void setPrivate_(boolean private_) {
    this.private_ = private_;
  }

  public String getScheme() {
    return scheme;
  }

  public void setScheme(String scheme) {
    this.scheme = scheme;
  }

  public String getTilejson() {
    return tilejson;
  }

  public void setTilejson(String tilejson) {
    this.tilejson = tilejson;
  }

  public String[] getTiles() {
    return tiles;
  }

  public void setTiles(String[] tiles) {
    this.tiles = tiles;
  }

  public VectorLayer[] getVectorLayers() {
    return vectorLayers;
  }

  public void setVectorLayers(VectorLayer[] vectorLayers) {
    this.vectorLayers = vectorLayers;
  }

  public static class VectorLayer {

    private String id;
    private String description;

    public VectorLayer() {}

    public VectorLayer(String id, String description) {
      this.id = id;
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public String getId() {
      return id;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public void setId(String id) {
      this.id = id;
    }
  }

  public static final class TileJsonBuilder {

    private String attribution;
    private double[] bounds = new double[]{-180, -SphericalMercator.MAX_LATITUDE, 180, SphericalMercator.MAX_LATITUDE};
    private int[] center = new int[]{0,0,0};
    private String description;
    private String filesize;
    private String format = "pbf";
    private String id;
    private int maskLevel = 22;
    private int maxzoom = 22;
    private int minzoom = 0;
    private String name;
    private boolean private_;
    private String scheme = "xyx";
    private String tilejson = "2.0.0";
    private String[] tiles;
    private VectorLayer[] vectorLayers;

    public static TileJsonBuilder newBuilder() { return new TileJsonBuilder();}

    private TileJsonBuilder() {}

    public TileJsonBuilder withAttribution(String attribution) {
      this.attribution = attribution;
      return this;
    }

    public TileJsonBuilder withBounds(double[] bounds) {
      this.bounds = bounds;
      return this;
    }

    public TileJsonBuilder withCenter(int[] center) {
      this.center = center;
      return this;
    }

    public TileJsonBuilder withDescription(String description) {
      this.description = description;
      return this;
    }

    public TileJsonBuilder withFilesize(String filesize) {
      this.filesize = filesize;
      return this;
    }

    public TileJsonBuilder withFormat(String format) {
      this.format = format;
      return this;
    }

    public TileJsonBuilder withId(String id) {
      this.id = id;
      return this;
    }

    public TileJsonBuilder withMaskLevel(int maskLevel) {
      this.maskLevel = maskLevel;
      return this;
    }

    public TileJsonBuilder withMaxzoom(int maxzoom) {
      this.maxzoom = maxzoom;
      return this;
    }

    public TileJsonBuilder withMinzoom(int minzoom) {
      this.minzoom = minzoom;
      return this;
    }

    public TileJsonBuilder withName(String name) {
      this.name = name;
      return this;
    }

    public TileJsonBuilder withPrivate_(boolean private_) {
      this.private_ = private_;
      return this;
    }

    public TileJsonBuilder withScheme(String scheme) {
      this.scheme = scheme;
      return this;
    }

    public TileJsonBuilder withTilejson(String tilejson) {
      this.tilejson = tilejson;
      return this;
    }

    public TileJsonBuilder withTiles(String[] tiles) {
      this.tiles = tiles;
      return this;
    }

    public TileJsonBuilder withVectorLayers(VectorLayer[] vectorLayers) {
      this.vectorLayers = vectorLayers;
      return this;
    }

    public TileJson build() {
      TileJson tileJson = new TileJson();
      tileJson.setAttribution(attribution);
      tileJson.setBounds(bounds);
      tileJson.setCenter(center);
      tileJson.setDescription(description);
      tileJson.setFilesize(filesize);
      tileJson.setFormat(format);
      tileJson.setId(id);
      tileJson.setMaskLevel(maskLevel);
      tileJson.setMaxzoom(maxzoom);
      tileJson.setMinzoom(minzoom);
      tileJson.setName(name);
      tileJson.setPrivate_(private_);
      tileJson.setScheme(scheme);
      tileJson.setTilejson(tilejson);
      tileJson.setTiles(tiles);
      tileJson.setVectorLayers(vectorLayers);
      return tileJson;
    }
  }
}
