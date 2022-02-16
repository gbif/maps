#occurrence {
  dot-width: 2;
                 [total <=     5] { dot-fill: #509E2F; }
  [total >     5][total <=    10] { dot-fill: #63A946; }
  [total >    10][total <=    50] { dot-fill: #76B35D; }
  [total >    50][total <=   100] { dot-fill: #8ABE74; }
  [total >   100][total <=   500] { dot-fill: #9DC98A; }
  [total >   500][total <=  1000] { dot-fill: #C2DDB6; }
  [total >  1000][total <=  5000] { dot-fill: #D5E8CD; }
  [total >  5000][total <= 10000] { dot-fill: #EDF5EA; }
  [total > 10000]                 { dot-fill: #FFFFFF; }
}
