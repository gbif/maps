#occurrence {
                 [total <=     5] { polygon-fill: #5e0063; }
  [total >     5][total <=    10] { polygon-fill: #851362; }
  [total >    10][total <=    50] { polygon-fill: #a42e61; }
  [total >    50][total <=   100] { polygon-fill: #be4c60; }
  [total >   100][total <=   500] { polygon-fill: #d26b63; }
  [total >   500][total <=  1000] { polygon-fill: #e28b6b; }
  [total >  1000][total <=  5000] { polygon-fill: #eeab79; }
  [total >  5000][total <= 10000] { polygon-fill: #f7cb8e; }
  [total > 10000]                 { polygon-fill: #ffebaa; }

  // Avoid uncoloured slivers between hexagons
  polygon-gamma: 0.2;
}
