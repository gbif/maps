/*
 * Purple to white, imitating the original V1 maps.
 */

#occurrence {
                 [total <=     5] { polygon-fill: #a000a0;  }
  [total >     5][total <=    10] { polygon-fill: #ab1fab;  }
  [total >    10][total <=    50] { polygon-fill: #b73fb7;  }
  [total >    50][total <=   100] { polygon-fill: #c35fc3;  }
  [total >   100][total <=   500] { polygon-fill: #cf7fcf;  }
  [total >   500][total <=  1000] { polygon-fill: #db9fdb;  }
  [total >  1000][total <=  5000] { polygon-fill: #e7bfe7;  }
  [total >  5000][total <= 10000] { polygon-fill: #f3dff3;  }
  [total > 10000]                 { polygon-fill: #ffffff;  }
}
