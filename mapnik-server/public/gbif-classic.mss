#OBSERVATION {
  marker-fill-opacity: 1;
  marker-line-width: 0;
  marker-type: ellipse;
  marker-width: 2;
  marker-fill: #CC0000;
  marker-allow-overlap: true;
}

#OBSERVATION {
  [count <= 100000] { marker-fill: #FF3300;  }
  [count <= 10000] { marker-fill: #FF6600;  }
  [count <= 1000] { marker-fill: #FF9900;  }
  [count <= 100] { marker-fill: #FFCC00;  }
  [count <= 10] { marker-fill: #FFFF00;  }
}

