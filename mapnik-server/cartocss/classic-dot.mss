#occurrence {
  dot-width: 2;
  [total <= 10] { dot-fill: #FFFF00;  }
  [total > 10][total <= 100] { dot-fill: #FFCC00;  }
  [total > 100][total <= 1000] { dot-fill: #FF9900;  }
  [total > 1000][total <= 10000] { dot-fill: #FF6600;  }
  [total > 10000] { dot-fill: #D60A00;  }
}
