#occurrence {
  marker-width: 4;
  marker-line-width: 0;
  marker-allow-overlap: true;
  marker-fill: #206EFF;
  marker-comp-op: screen;

                 [total <=    10] { marker-fill: #5E0700;  }
  [total >    10][total <=   100] { marker-fill: #EF4712;  }
  [total >   100][total <=  1000] { marker-fill: #DC6902;  }
  [total >  1000][total <= 10000] { marker-fill: #F09C00;  }
  [total > 10000]                 { marker-fill: #F2F7F0;  }

  [zoom >= 11] { marker-width:  3.95; }
  [zoom >= 12] { marker-width:  6.25; }
  [zoom >= 13] { marker-width:  9.88; }
  [zoom >= 14] { marker-width:  15.63; }
  [zoom >= 15] { marker-width:  24.71; }

}
