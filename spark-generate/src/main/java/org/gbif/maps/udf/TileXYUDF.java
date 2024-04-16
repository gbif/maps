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
package org.gbif.maps.udf;

import org.gbif.maps.common.projection.*;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;

/**
 * Returns the tiles addresses for the given global coordinate. Tiles have a buffer size, and so a
 * point can fall on up to four tiles in corner regions.
 */
@AllArgsConstructor
public class TileXYUDF implements UDF3<Integer, Integer, Integer, Row[]>, Serializable {
  enum Direction {
    N,
    S,
    E,
    W,
    NE,
    NW,
    SE,
    SW
  };

  final String epsg;
  final int tileSize;
  final int bufferSize;

  public static void register(
      SparkSession spark, String name, String epsg, int tileSize, int bufferSize) {
    spark
        .udf()
        .register(
            name,
            new TileXYUDF(epsg, tileSize, bufferSize),
            DataTypes.createArrayType(
                DataTypes.createStructType(
                    new StructField[] {
                      // Designed to work until zoom 16; higher requires tileXY to be LongType
                      DataTypes.createStructField("tileX", DataTypes.IntegerType, false),
                      DataTypes.createStructField("tileY", DataTypes.IntegerType, false),
                      DataTypes.createStructField("pixelX", DataTypes.IntegerType, false),
                      DataTypes.createStructField("pixelY", DataTypes.IntegerType, false)
                    })));
  }

  @Override
  public Row[] call(Integer zoom, Integer x, Integer y) {
    List<String> addresses = Lists.newArrayList();
    Double2D globalXY = new Double2D(x, y);

    // Readdress coordinates onto their primary tile
    TileSchema tileSchema = TileSchema.fromSRS(epsg);
    Long2D tileXY = Tiles.toTileXY(globalXY, tileSchema, zoom, tileSize);
    Long2D localXY =
        Tiles.toTileLocalXY(
            globalXY, tileSchema, zoom, tileXY.getX(), tileXY.getY(), tileSize, bufferSize);
    append(addresses, tileXY, localXY);

    // Readdress coordinates that fall in the buffer region of adjacent tiles
    readdressAndAppend(
        addresses, tileSchema, globalXY, tileXY, zoom, localXY.getX(), localXY.getY());

    Set<Row> rows =
        addresses.stream()
            .map(
                s -> {
                  String p[] = s.split(",");
                  return RowFactory.create(
                      Integer.valueOf(p[0]),
                      Integer.valueOf(p[1]),
                      Integer.valueOf(p[2]),
                      Integer.valueOf(p[3]));
                })
            .collect(Collectors.toSet());

    return rows.toArray(new Row[rows.size()]);
  }

  /**
   * Takes the original x,y and if it lies within the boundary of adjacent tiles, adds the address
   * of the pixel.
   */
  void readdressAndAppend(
      List<String> target,
      TileSchema tileSchema,
      Double2D globalXY,
      Long2D tileXY,
      int zoom,
      long x,
      long y) {

    // this will collect multiple times, but are collected into a Set to distinct later
    if (y < bufferSize) {
      appendOnTile(
          target,
          tileSchema,
          globalXY,
          zoom,
          adjacentTileAddress(tileSchema, zoom, Direction.N, tileXY));

      if (x < bufferSize) {
        appendOnTile(
            target,
            tileSchema,
            globalXY,
            zoom,
            adjacentTileAddress(tileSchema, zoom, Direction.NW, tileXY));
      }

      if (x >= tileSize - bufferSize) {
        appendOnTile(
            target,
            tileSchema,
            globalXY,
            zoom,
            adjacentTileAddress(tileSchema, zoom, Direction.NE, tileXY));
      }
    }
    if (x >= tileSize - bufferSize) {
      appendOnTile(
          target,
          tileSchema,
          globalXY,
          zoom,
          adjacentTileAddress(tileSchema, zoom, Direction.E, tileXY));
    }
    if (y >= tileSize - bufferSize) {
      appendOnTile(
          target,
          tileSchema,
          globalXY,
          zoom,
          adjacentTileAddress(tileSchema, zoom, Direction.S, tileXY));

      if (x < bufferSize) {
        appendOnTile(
            target,
            tileSchema,
            globalXY,
            zoom,
            adjacentTileAddress(tileSchema, zoom, Direction.SW, tileXY));
      }
      if (x >= tileSize - bufferSize) {
        appendOnTile(
            target,
            tileSchema,
            globalXY,
            zoom,
            adjacentTileAddress(tileSchema, zoom, Direction.SE, tileXY));
      }
    }
    if (x < bufferSize) {
      appendOnTile(
          target,
          tileSchema,
          globalXY,
          zoom,
          adjacentTileAddress(tileSchema, zoom, Direction.W, tileXY));
    }
  }

  private void appendOnTile(
      List<String> target, TileSchema tileSchema, Double2D globalXY, int zoom, Long2D tileXY) {
    Long2D localXY =
        Tiles.toTileLocalXY(
            globalXY, tileSchema, zoom, tileXY.getX(), tileXY.getY(), tileSize, bufferSize);
    append(target, tileXY, localXY);
  }

  private static void append(List<String> addresses, Long2D tileXY, Long2D localXY) {
    addresses.add(
        String.join(
            ",",
            String.valueOf(tileXY.getX()),
            String.valueOf(tileXY.getY()),
            String.valueOf(localXY.getX()),
            String.valueOf(localXY.getY())));
  }

  /** Returns the tile X,Y coordinates for the adjacent tile in the given direction. */
  static Long2D adjacentTileAddress(
      TileSchema schema, int zoom, Direction direction, Long2D origin) {
    int numXTiles = (1 << zoom) * schema.getZzTilesHorizontal();
    int numYTiles = (1 << zoom) * schema.getZzTilesVertical();
    long x = origin.getX();
    long y = origin.getY();

    // find the vertical tile offset, wrapping around the polar regions
    if (direction == Direction.N || direction == Direction.NE || direction == Direction.NW) {
      y = y - 1 < 0 ? numYTiles - 1 : y - 1;
    } else if (direction == Direction.S || direction == Direction.SE || direction == Direction.SW) {
      y = y + 1 >= numYTiles ? 0 : y + 1;
    }

    if (direction == Direction.W || direction == Direction.NW || direction == Direction.SW) {
      x = x - 1 < 0 ? numXTiles - 1 : x - 1;
    } else if (direction == Direction.E || direction == Direction.NE || direction == Direction.SE) {
      x = x + 1 >= numXTiles ? 0 : x + 1;
    }

    return new Long2D(x, y);
  }
}
