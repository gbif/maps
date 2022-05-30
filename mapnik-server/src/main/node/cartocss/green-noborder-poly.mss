#occurrence {
                  [total <=      5] { polygon-fill: #467B31;  }
  [total >      5][total <=     10] { polygon-fill: #588740;  }
  [total >     10][total <=     50] { polygon-fill: #76B35D;  }
  [total >     50][total <=    100] { polygon-fill: #7B9E5D;  }
  [total >    100][total <=    500] { polygon-fill: #8FAA6D;  }
  [total >    500][total <=   1000] { polygon-fill: #A1B57D;  }
  [total >   1000][total <=   5000] { polygon-fill: #B5C28F;  }
  [total >   5000][total <=  10000] { polygon-fill: #CACEA1;  }
  [total >  10000][total <= 100000] { polygon-fill: #DFDAB2;  }
  [total > 100000]                  { polygon-fill: #F4E7C5;  }

  // Avoid uncoloured slivers between hexagons
  polygon-gamma: 0.2;
}
