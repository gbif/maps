#occurrence {
  dot-width: 2;
  dot-opacity: 1;
  dot-fill: #CC0000;
}

#occurrence {
  [total <= 100000] { dot-fill: #FF3300;  }
  [total <= 10000] { dot-fill: #FF6600;  }
  [total <= 1000] { dot-fill: #FF9900;  }
  [total <= 100] { dot-fill: #FFCC00;  }
  [total <= 10] { dot-fill: #FFFF00;  }
}

