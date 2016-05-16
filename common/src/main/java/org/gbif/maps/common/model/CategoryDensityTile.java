package org.gbif.maps.common.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * A highly optimized content model to capture a tile containing pixels of counts by year grouped into categories.
 * This works by encoding the pixel x, pixel y, category, year and count into a long (64 bits as follows) and storing
 * a count for that key.
 *
 * This assumes that the category can be binned into a total of 2^31 categories, and that pixel addresses are guaranteed
 * to fit into 4 bytes (65536 x 65536 being the largest address).
 * <p/>
 * This class is mutable, and is not threadsafe.
 * <p/>
 * Experimental: Use with caution.
 */
public final class CategoryDensityTile implements Serializable {
  // large since pixel+year+basisOfRecord result in a lot of combinations
  private static final int DEFAULT_SIZE = 10000;

  private final Long2IntMap data;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public CategoryDensityTile() {
    this(DEFAULT_SIZE);
  }

  public CategoryDensityTile(int expectedSize) {
    data = new Long2IntOpenHashMap(expectedSize);;
  }

  /**
   * Collects a piece of data into the tile.  If the given combination already exists, the count is incremented.
   * @param x The pixel x address
   * @param y The pixel y address
   * @param category The category to collect into
   * @param year The year
   * @param count The count at the combination of all other parameters
   * @return this instance
   */
  public CategoryDensityTile collect(short x, short y, short category, short year, int count) {
    long key = encodeKey(x,y,category,year);
    if (data.containsKey(key)) {
      data.put(key, data.get(key) + count);
    } else {
      data.put(key, count);
    }
    return this;
  }

  public byte[] toVectorTile() {
    VectorTileEncoder encoder = new VectorTileEncoder(4096, 0, false); // for each entry (a pixel, px) we have a year:count Map
    /*
    // TODO: this properly
    IntSet pixels = new IntOpenHashSet();
    LongIterator iter = data.keySet().iterator();
    while (iter.hasNext()) {
      pixels.add((int) (iter.nextLong() >> 32));
    }
    IntIterator iter2 = pixels.iterator();
    while (iter2.hasNext()) {
      long px = iter2.next();
      int x = (int)(px>>16);
      int y = (int)(px & 0xFFFF);
      Point p = GEOMETRY_FACTORY.createPoint(new Coordinate((double)x, (double)y));
      Map<String, Object> meta = new HashMap();
      encoder.addFeature("points", meta, p);
    }
    */
    Point p = GEOMETRY_FACTORY.createPoint(new Coordinate((double)10, (double)10));
    Map<String, Object> meta = new HashMap();
    encoder.addFeature("points", meta, p);
    return encoder.encode();
  }

  /**
   * Wrapper around collect(short,short,short,short,int) to save adding casting everywhere.
   */
  public CategoryDensityTile collect(int x, int y, int category, int year, int count) {
    return collect((short) x, (short) y, (short) category, (short) year, count);
  }

  /**
   * Appends all data from the source into this tile.
   * @return this
   */
  public CategoryDensityTile collectAll(CategoryDensityTile source) {
    for (Long2IntMap.Entry e : source.data.long2IntEntrySet()) {
      if (data.containsKey(e.getLongKey())) {
        data.put(e.getLongKey(), data.get(e.getLongKey()) + e.getIntValue());
      } else {
        data.put(e.getLongKey(), e.getIntValue());
      }
    }
    return this;
  }

  /**
   * Returns a new tile downscaled by 1 zoom level from the provided tile.
   * @param source To downscale
   * @param z The zoom level of the source
   * @param x The tile x address of the source
   * @param y The tile y address of the source
   * @param tileSize The pixel width of the tile (i.e. 4096x4096 tile would be 4096)
   * @return A new tile, suitable for use at 1 zoom lower than the given
   */
  public CategoryDensityTile downscale(int z, int x, int y, int tileSize) {
    if (z<=0) throw new IllegalArgumentException("Z must be greater than 0: " + z);

    if (true) return this;

    // assume it will shrink to around a quarter size
    CategoryDensityTile result = new CategoryDensityTile(data.size() / 4);

    // The source tile will contribute to 1 quadrant of the result tile.
    // The offsets locate which quadrant it is in.
    int offsetX = (short) (tileSize/2 * (x%2));
    int offsetY = (short) (tileSize/2 * (y%2));

    for (Long2IntMap.Entry e : data.long2IntEntrySet()) {
      short[] pixel = decodeKey(e.getLongKey());
      short px = (short)(offsetX + (pixel[0]/2));
      short py = (short)(offsetY + (pixel[1]/2));
      result.collect(px,py,pixel[2],pixel[3],e.getIntValue());
    }
    return result;
  }

  /**
   * Encodes the key in this order:
   * <ol>
   *   <li>First 16 bits represent pixel address x</li>
   *   <li>Next 16 bits represent pixel address y</li>
   *   <li>Next 16 bits represent the category</li>
   *   <li>Next 16 bits represent the year</li>
   * </ol>
   */
  static final long encodeKey(short x, short y, short category, short year) {
    return (long)x<<48 | (long)y<<32 | (long)category<<16 | (long)year;
  }

  /**
   * Decodes the pixel into an array of x,y,category,year.
   */
  public static final short[] decodeKey(long key) {
    return new short[]{
      (short)(key>>48),
      (short)(key>>32 & 0xFFFF),
      (short)(key>>16 & 0xFFFF),
      (short)(key & 0xFFFF)
    };
  }

  /**
   * Intended to aid testing purpose only.
   * Returns a CSV formated, sorted view of the data.
   */
  String toDebugString() {
    List<String> l = new ArrayList();
    for (Long2IntMap.Entry e : data.long2IntEntrySet()) {
      short[] key = decodeKey(e.getLongKey());
      l.add(key[0] + "," + key[1] + "," + key[2] + "," + key[3] + "," + e.getIntValue() + "\\n");
    }
    Collections.sort(l);
    StringBuffer sb = new StringBuffer();
    for (String s : l) {
      sb.append(s);
    }
    return sb.toString();
  }
}
