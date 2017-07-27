#occurrence {
  dot-width: 2;
                  [total <=     10] { dot-fill: #F7005A; opacity: 90%; }
  [total >     10][total <=    100] { dot-fill: #D50067; opacity: 90%; }
  [total >    100][total <=   1000] { dot-fill: #B5006C; opacity: 90%; }
  [total >   1000][total <=  10000] { dot-fill: #94006A; opacity: 90%; }
  [total >  10000][total <= 100000] { dot-fill: #72005F; opacity: 90%; }
  [total > 100000]                  { dot-fill: #52034E; opacity: 90%; }
}
