#OBSERVATION {
  dot-width: 2;
  dot-opacity: 1;
  dot-fill: #CC0000;
}

#OBSERVATION {
  [count <= 100000] { dot-fill: #FF3300;  }
  [count <= 10000] { dot-fill: #FF6600;  }
  [count <= 1000] { dot-fill: #FF9900;  }
  [count <= 100] { dot-fill: #FFCC00;  }
  [count <= 10] { dot-fill: #FFFF00;  }
}
