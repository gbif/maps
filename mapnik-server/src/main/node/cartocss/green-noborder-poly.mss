#occurrence {
                  [total <=      5] { polygon-fill: #4D7C40; }
  [total >      5][total <=     10] { polygon-fill: #608A50; }
  [total >     10][total <=     50] { polygon-fill: #70955E; }
  [total >     50][total <=    100] { polygon-fill: #85A36F; }
  [total >    100][total <=    500] { polygon-fill: #95AD7C; }
  [total >    500][total <=   1000] { polygon-fill: #AABA8D; }
  [total >   1000][total <=   5000] { polygon-fill: #BAC599; }
  [total >   5000][total <=  10000] { polygon-fill: #CCD0A8; }
  [total >  10000][total <= 100000] { polygon-fill: #DFDCB7; }
  [total > 100000]                  { polygon-fill: #EFE6C4; }

  // Avoid uncoloured slivers between hexagons
  polygon-gamma: 0.2;
}
