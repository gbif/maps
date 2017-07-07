#occurrence {
  polygon-fill: #006d2c;
  opacity: 0.9;
  line-color: "#7b7b7b";
  line-width: 1;
  line-opacity: 0.5;
}

#occurrence {
                 [total <=    10] { polygon-fill: #FFFF00;  }
  [total >    10][total <=   100] { polygon-fill: #FFCC00;  }
  [total >   100][total <=  1000] { polygon-fill: #FF9900;  }
  [total >  1000][total <= 10000] { polygon-fill: #FF6600;  }
  [total > 10000]                 { polygon-fill: #D60A00;  }
}
