#occurrence {
  marker-width: 1;
  marker-line-width: 0;
  marker-allow-overlap: true;
  marker-fill: #206EFF;
  marker-comp-op: screen;

                 [total <=    10] { marker-fill: #045a8d;  }
  [total >    10][total <=   100] { marker-fill: #2b8cbe;  }
  [total >   100][total <=  1000] { marker-fill: #74a9cf;  }
  [total >  1000][total <= 10000] { marker-fill: #bdc9e1;  }
  [total > 10000]                 { marker-fill: #f1eef6;  }

  [zoom >= 11] { marker-width:  3.95; }
  [zoom >= 12] { marker-width:  6.25; }
  [zoom >= 13] { marker-width:  9.88; }
  [zoom >= 14] { marker-width:  15.63; }
  [zoom >= 15] { marker-width:  24.71; }

}
