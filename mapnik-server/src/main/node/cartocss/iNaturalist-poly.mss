#occurrence {
  opacity: 0.9;

                  [total <=     10] { polygon-fill: #F7005A; }
  [total >     10][total <=    100] { polygon-fill: #D50067; }
  [total >    100][total <=   1000] { polygon-fill: #B5006C; }
  [total >   1000][total <=  10000] { polygon-fill: #94006A; }
  [total >  10000][total <= 100000] { polygon-fill: #72005F; }
  [total > 100000]                  { polygon-fill: #52034E; }
}
