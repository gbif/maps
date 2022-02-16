#occurrence {
                 [total <=     5] { polygon-fill: #509E2F; }
  [total >     5][total <=    10] { polygon-fill: #63A946; }
  [total >    10][total <=    50] { polygon-fill: #76B35D; }
  [total >    50][total <=   100] { polygon-fill: #8ABE74; }
  [total >   100][total <=   500] { polygon-fill: #9DC98A; }
  [total >   500][total <=  1000] { polygon-fill: #C2DDB6; }
  [total >  1000][total <=  5000] { polygon-fill: #D5E8CD; }
  [total >  5000][total <= 10000] { polygon-fill: #EDF5EA; }
  [total > 10000]                 { polygon-fill: #FFFFFF; }

  // Avoid uncoloured slivers between hexagons
  polygon-gamma: 0.2;
}
