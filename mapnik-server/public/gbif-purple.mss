#OBSERVATION {
  marker-fill-opacity: 1;
  marker-line-width: 0;
  marker-type: ellipse;
  marker-width: 2;
  marker-fill: #FFFFFF;
  marker-allow-overlap: true;
}

#OBSERVATION {
  [count <= 100000] { marker-fill: #EDC8F1;  }
  [count <= 10000] { marker-fill: #E0A1E6;  }
  [count <= 1000] { marker-fill: #DF9EE5;  }
  [count <= 100] { marker-fill: #C75DD0;  }
  [count <= 10] { marker-fill: #A20BAF;  }
}
