/*
 * White to green, imitating the original V1 map style "greens".
 */

#occurrence {
                 [total <=    10] { polygon-fill: #edf8e9; }
  [total >    10][total <=   100] { polygon-fill: #bae4b3; }
  [total >   100][total <=  1000] { polygon-fill: #74c476; }
  [total >  1000][total <= 10000] { polygon-fill: #31a354; }
  [total > 10000]                 { polygon-fill: #006d2c; }
}
