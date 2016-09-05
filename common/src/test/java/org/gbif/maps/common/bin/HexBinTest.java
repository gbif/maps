package org.gbif.maps.common.bin;

import org.codetome.hexameter.core.api.Hexagon;
import org.codetome.hexameter.core.api.HexagonalGrid;
import org.junit.Test;
import rx.functions.Action1;

import static org.junit.Assert.*;

public class HexBinTest {

  @Test
  public void testNewGridInstance() {
    HexBin bin = new HexBin(4096, 5);
    HexagonalGrid grid = bin.newGridInstance();


    grid.getHexagons().forEach(new Action1<Hexagon>() {
      @Override
      public void call(Hexagon hexagon) {

        System.out.println(hexagon.getGridX() + "," + hexagon.getGridY() + "," + hexagon.getGridZ());
      }
    });

  }

}
