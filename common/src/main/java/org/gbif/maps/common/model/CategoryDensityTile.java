package org.gbif.maps.common.model;

import org.gbif.maps.common.projection.Mercator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2IntArrayMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntRBTreeMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
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
  private static final int DEFAULT_SIZE = 100000;
  private static final long serialVersionUID = -1446501648367890366L;

  private Long2IntMap data;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public static void main(String[] args) {
    Random r = new Random();
    long time = System.currentTimeMillis();
    CategoryDensityTile t1 = new CategoryDensityTile();

    int size = 100000;

    for (int i=0; i<size; i++) {
      t1.collect(r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt());
    }

    CategoryDensityTile t2 = new CategoryDensityTile();
    for (int i=0; i<size; i++) {
      t2.collect(r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt());
    }


    System.out.println("Collecting...");
    CategoryDensityTile t3 = collectAll(t1, t2);
    new CategoryDensityTile(t1,t2);
    System.out.println("Duration: " + (System.currentTimeMillis() - time));

    try {
      time = System.currentTimeMillis();
      FileOutputStream fileOut = new FileOutputStream("/tmp/delme.ser");
      ObjectOutputStream out = new ObjectOutputStream(fileOut);
      out.writeObject(t3);
      out.close();
      fileOut.close();

      FileInputStream fileIn = new FileInputStream("/tmp/delme.ser");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      CategoryDensityTile unser = (CategoryDensityTile) in.readObject();
      in.close();
      fileIn.close();

      System.out.println("SerDe of " + unser.data.size() + " took " + (System.currentTimeMillis() - time));

      time = System.currentTimeMillis();

      Pattern TAB = Pattern.compile("y");
      BufferedReader reader = new BufferedReader(new FileReader("/tmp/latLngBoRYCount.txt"));
      String line = reader.readLine();
      CategoryDensityTile all = new CategoryDensityTile();
      Mercator m = new Mercator(4096);
      Map<String, Short> bor = new HashMap();
      bor.put("UNKNOWN",(short)0);
      bor.put("\\N",(short)0);
      bor.put("PRESERVED_SPECIMEN",(short)1);
      bor.put("FOSSIL_SPECIMEN",(short)2);
      bor.put("LIVING_SPECIMEN",(short)3);
      bor.put("OBSERVATION",(short)4);
      bor.put("HUMAN_OBSERVATION",(short)5);
      bor.put("MACHINE_OBSERVATION",(short)6);
      bor.put("MATERIAL_SAMPLE",(short)7);
      bor.put("LITERATURE",(short)8);
      while (line != null) {
        String[] a = TAB.split(line);
        line = reader.readLine();
        double lat = Double.valueOf(a[0]);
        double lng = Double.valueOf(a[1]);
        if (lng>0) continue;
        if (lat<0) continue;

        short b = bor.containsKey(a[3]) ? bor.get(a[3]) : 0;
        short year = "\\N".equals(a[2]) ? -1 : Short.valueOf(a[2]);
        all.collect(
          (short)m.longitudeToTileLocalPixelX(lng, (byte)1),
          (short)m.latitudeToTileLocalPixelY(lat, (byte)1),
          b,
          year,
          Integer.valueOf(a[4])
        );


      }
      System.out.println("Encoding of " + all.data.size() + " took " + (System.currentTimeMillis() - time));


    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Constructs a tile from the supplied tiles which should not overlap in features.
   * Any overlap will result in the values from t2 being used.  This is intended to be used when t1 represents
   * one quadrant of data in a tile and t2 represents another quadrant.
   */
  public CategoryDensityTile(CategoryDensityTile t1, CategoryDensityTile t2) {
    data = new Long2IntOpenHashMap(t1.data.size() + t2.data.size());
    data.putAll(t1.data);
    data.putAll(t2.data);
  }

  public CategoryDensityTile() {
    this(DEFAULT_SIZE);
  }

  public CategoryDensityTile(int expectedSize) {
    data = new Long2IntOpenHashMap(expectedSize);
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
    // TODO: this properly
    IntSet pixels = new IntOpenHashSet(100000);
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
    return encoder.encode();
  }

  /**
   * Wrapper around collect(short,short,short,short,int) to save adding casting everywhere.
   */
  public CategoryDensityTile collect(int x, int y, int category, int year, int count) {
    return collect((short) x, (short) y, (short) category, (short) year, count);
  }

  /**
   * Appends all data from the source into a new tile.
   * @return this
   */
  public static CategoryDensityTile collectAll(CategoryDensityTile t1, CategoryDensityTile t2) {
    long time = System.currentTimeMillis();
    // Performance is awful unless this is initialised with a suitablly large size(!)
    CategoryDensityTile t = new CategoryDensityTile(t1.data.size() + t2.data.size());

    // load t1 in entirity
    t.data.putAll(t1.data);

    // merge in t2
    for (Long2IntMap.Entry e : t2.data.long2IntEntrySet()) {
      if (t.data.containsKey(e.getLongKey())) {
        t.data.put(e.getLongKey(), t.data.get(e.getLongKey()) + e.getIntValue());
      } else {
        t.data.put(e.getLongKey(), e.getIntValue());
      }
    }
    System.out.println("collectAll(m1,m2) produced[" + t.data.size() + "] in " + (System.currentTimeMillis() - time));
    return t;
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
  public static CategoryDensityTile downscale(CategoryDensityTile orig, int z, int x, int y, int tileSize) {
    if (z<=0) throw new IllegalArgumentException("Z must be greater than 0: " + z);

    // assume it will shrink to around half the size
    CategoryDensityTile result = new CategoryDensityTile(orig.data.size() / 2);

    // The source tile will contribute to 1 quadrant of the result tile.
    // The offsets locate which quadrant it is in.
    int offsetX = (short) (tileSize/2 * (x%2));
    int offsetY = (short) (tileSize/2 * (y%2));

    for (Long2IntMap.Entry e : orig.data.long2IntEntrySet()) {
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

  @Override
  public int hashCode() {
    return data.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CategoryDensityTile that = (CategoryDensityTile) o;
    if (!data.equals(that.data)) return false;
    return true;
  }


  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    data = (Long2IntMap) BinIO.loadObject(in);
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    BinIO.storeObject(data, out);
  }
}
