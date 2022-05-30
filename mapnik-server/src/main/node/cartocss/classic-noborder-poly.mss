#occurrence {
                 [total <=    10] { polygon-fill: #FFFF00; }
  [total >    10][total <=   100] { polygon-fill: #FFCC00; }
  [total >   100][total <=  1000] { polygon-fill: #FF9900; }
  [total >  1000][total <= 10000] { polygon-fill: #FF6600; }
  [total > 10000]                 { polygon-fill: #D60A00; }

  // Avoid uncoloured slivers between hexagons
  polygon-gamma: 0.2;
}
