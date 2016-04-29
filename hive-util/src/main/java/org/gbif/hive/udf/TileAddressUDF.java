package org.gbif.hive.udf;

import org.gbif.maps.common.projection.Mercator;

import java.util.Arrays;

import com.google.common.cache.CacheLoader;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * Takes a coordinate in a WGS-84 reference system and provides the tile address (x,y) at the given zoom in Mercator
 * projection.
 */
@Description(
  name = "tileAddressUDF",
  value = "_FUNC_(tileSize, zoom, latitude, longitude)")
public class TileAddressUDF extends GenericUDF {

  // Reuse the projection utility.
  private static final CacheLoader<Integer, Mercator> CACHE = new CacheLoader<Integer, Mercator>() {
    @Override
    public Mercator load(Integer tileSize) {
      return new Mercator(tileSize);
    }
  };

  private ObjectInspectorConverters.Converter[] converters;

  /**
   * Setup converters for reading input and response data.
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 4) {
      throw new UDFArgumentException("tileAdressUDF takes four arguments");
    }

    // converters for the input data
    converters = new ObjectInspectorConverters.Converter[] {
      ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.writableIntObjectInspector),
      ObjectInspectorConverters.getConverter(arguments[1], PrimitiveObjectInspectorFactory.writableIntObjectInspector),
      ObjectInspectorConverters.getConverter(arguments[2], PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
      ObjectInspectorConverters.getConverter(arguments[3], PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)
    };

    // response data provides x,y address for the tile
    return ObjectInspectorFactory
      .getStandardStructObjectInspector(Arrays.asList("x", "y"),
                                        Arrays.<ObjectInspector>asList(
                                          PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                                          PrimitiveObjectInspectorFactory.javaLongObjectInspector));
  }

  /**
   * Convert the Latitude, Longitude to the tile address
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert arguments.length == 4;

    IntWritable tileSize = (IntWritable) converters[0].convert(arguments[0].get());
    IntWritable zoom = (IntWritable) converters[1].convert(arguments[1].get());
    Double latitude = (Double) converters[2].convert(arguments[2].get());
    Double longitude = (Double) converters[3].convert(arguments[3].get());

    if (tileSize == null || zoom == null || latitude == null || longitude == null) {
      return null; // nothing to convert
    }

    try {
      Mercator mercator = CACHE.load(tileSize.get());
      return Arrays.<Object>asList(
        mercator.longitudeToTileX(longitude, (byte) zoom.get()),
        mercator.latitudeToTileY(latitude, (byte) zoom.get()));

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public String getDisplayString(String[] strings) {
    assert strings.length == 4;
    return "tileAdressUDF(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ", " + strings[3] + ')';
  }
}
