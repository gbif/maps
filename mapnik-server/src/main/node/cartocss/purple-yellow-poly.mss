#occurrence {
  polygon-fill: #006d2c;
  opacity: 0.9;
}

#occurrence {
                 [total <=     5] { polygon-fill: #5e0063; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total >     5][total <=    10] { polygon-fill: #851362; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total >    10][total <=    50] { polygon-fill: #a42e61; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total >    50][total <=   100] { polygon-fill: #be4c60; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total >   100][total <=   500] { polygon-fill: #d26b63; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total >   500][total <=  1000] { polygon-fill: #e28b6b; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total >  1000][total <=  5000] { polygon-fill: #eeab79; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total >  5000][total <= 10000] { polygon-fill: #f7cb8e; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
  [total > 10000]                 { polygon-fill: #ffebaa; line-color: "#7b7b7b"; line-width: 1; line-opacity: 0.5; }
}
