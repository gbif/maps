import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class Test {

  public static void main(String[] args) {
    // pixel to category to year to count
    Int2ObjectOpenHashMap pixels = new Int2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntMap>>();

    int pixel = 1;
    pixels.getOrDefault(pixel, new Int2ObjectOpenHashMap<Int2IntMap>());



  }
}
