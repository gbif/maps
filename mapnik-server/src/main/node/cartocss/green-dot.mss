#occurrence {
  dot-width: 2;
                  [total <=      5] { dot-fill: #467B31;  }
  [total >      5][total <=     10] { dot-fill: #588740;  }
  [total >     10][total <=     50] { dot-fill: #76B35D;  }
  [total >     50][total <=    100] { dot-fill: #7B9E5D;  }
  [total >    100][total <=    500] { dot-fill: #8FAA6D;  }
  [total >    500][total <=   1000] { dot-fill: #A1B57D;  }
  [total >   1000][total <=   5000] { dot-fill: #B5C28F;  }
  [total >   5000][total <=  10000] { dot-fill: #CACEA1;  }
  [total >  10000][total <= 100000] { dot-fill: #DFDAB2;  }
  [total > 100000]                  { dot-fill: #F4E7C5;  }
}
