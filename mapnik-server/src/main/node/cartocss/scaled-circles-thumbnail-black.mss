#occurrence {
  marker-line-width: 0;
  marker-allow-overlap: true;
  marker-fill: #000000;

                 [total <=    10] { marker-width: 25;  marker-fill: #000; marker-opacity: 1.0; marker-line-color: #000; marker-line-width: 1 }
  [total >    10][total <=   100] { marker-width: 28;  marker-fill: #000; marker-opacity: 0.8; marker-line-color: #000; marker-line-width: 0 }
  [total >   100][total <=  1000] { marker-width: 30;  marker-fill: #000; marker-opacity: 0.7; marker-line-color: #000; marker-line-width: 0 }
  [total >  1000][total <= 10000] { marker-width: 32;  marker-fill: #000; marker-opacity: 0.6; marker-line-color: #000; marker-line-width: 0 }
  [total > 10000]                 { marker-width: 35;  marker-fill: #000; marker-opacity: 0.6; marker-line-color: #000; marker-line-width: 0 }
}
