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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Projects the given lat,lng onto the global coordinates of the target project at the given zoom.
 * This code is a quick hack for demonstration, knowing it will be ported to Spark anyway.
 */
public class ProjectUDF extends GenericUDF {

  @Override
  public ObjectInspector initialize(ObjectInspector[] objectInspectors) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("x", "y"),
      Arrays.asList(
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
      )
    );
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) {
    // TODO: some type checking (e.g. nulls)
    try {
      double lat = ((DoubleWritable) arguments[0].get()).get();
      double lng = ((DoubleWritable) arguments[1].get()).get();
      String epsg = arguments[2].get().toString();
      int zoom = ((IntWritable) arguments[3].get()).get();

      TileProjection tp = Tiles.fromEPSG(epsg, 1024);
      Double2D xy = tp.toGlobalPixelXY(lat,lng,zoom);

      List<Object> result = new ArrayList<>(2);
      result.add(xy.getX());
      result.add(xy.getY());
      return result;

    } catch (Exception e) {
      e.printStackTrace();
      return null; // TODO error handling
    }
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "project(" + String.join(",", strings) + ")";
  }
}
