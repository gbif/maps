/*
 * Purple to white, imitating the original V1 maps.
 */

#occurrence {
  dot-width: 2;
  [total <= 5] { dot-fill: #a000a0;  }
  [total > 5][total <= 10] { dot-fill: #ab1fab;  }
  [total > 10][total <= 50] { dot-fill: #b73fb7;  }
  [total > 50][total <= 100] { dot-fill: #c35fc3;  }
  [total > 100][total <= 500] { dot-fill: #cf7fcf;  }
  [total > 500][total <= 1000] { dot-fill: #db9fdb;  }
  [total > 1000][total <= 50000] { dot-fill: #e7bfe7;  }
  [total > 5000][total <= 10000] { dot-fill: #f3dff3;  }
  [total > 10000] { dot-fill: #ffffff;  }
}
