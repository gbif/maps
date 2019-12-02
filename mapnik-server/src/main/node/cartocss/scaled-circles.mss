#occurrence {
  marker-line-width: 0;
  marker-allow-overlap: true;
  marker-fill: #206EFF;

                 [total <=    10] { marker-width: 6;  marker-fill: #fed976; marker-opacity: 1.0; marker-line-color: #fe9724; marker-line-width: 1 }
  [total >    10][total <=   100] { marker-width: 7;  marker-fill: #fd8d3c; marker-opacity: 0.8; marker-line-color: #fd5b24; marker-line-width: 0 }
  [total >   100][total <=  1000] { marker-width: 10;  marker-fill: #fd8d3c; marker-opacity: 0.7; marker-line-color: #fd471d; marker-line-width: 0 }
  [total >  1000][total <= 10000] { marker-width: 16;  marker-fill: #f03b20; marker-opacity: 0.6; marker-line-color: #f01129; marker-line-width: 0 }
  [total > 10000]                 { marker-width: 30; marker-fill: #bd0026; marker-opacity: 0.6; marker-line-color: #bd0047; marker-line-width: 0 }
}
