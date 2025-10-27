#occurrence {
                  [total <=     10] { polygon-fill: #FFFF00;  }
  [total >     10][total <=    100] { polygon-fill: #FFCC00;  }
  [total >    100][total <=   1000] { polygon-fill: #FF9900;  }
  [total >   1000][total <=  10000] { polygon-fill: #FF6600;  }
  [total >  10000][total <= 100000] { polygon-fill: #D60A00;  }
  [total > 100000]                  { polygon-fill: #C2002D;  }

  // The offset and gamma avoid the appearance of misalignment with squares.
  line-color: "#7b7b7b";
  line-width: 0.25;
  line-gamma: 0.5;
  line-opacity: 1.0;
  line-offset: -0.1;

  opacity: 0.9;
}
