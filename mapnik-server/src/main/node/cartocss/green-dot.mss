#occurrence {
  dot-width: 2;
                  [total <=      5] { dot-fill: #4D7C40; }
  [total >      5][total <=     10] { dot-fill: #608A50; }
  [total >     10][total <=     50] { dot-fill: #70955E; }
  [total >     50][total <=    100] { dot-fill: #85A36F; }
  [total >    100][total <=    500] { dot-fill: #95AD7C; }
  [total >    500][total <=   1000] { dot-fill: #AABA8D; }
  [total >   1000][total <=   5000] { dot-fill: #BAC599; }
  [total >   5000][total <=  10000] { dot-fill: #CCD0A8; }
  [total >  10000][total <= 100000] { dot-fill: #DFDCB7; }
  [total > 100000]                  { dot-fill: #EFE6C4; }
}
