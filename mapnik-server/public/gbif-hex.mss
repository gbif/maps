#OBSERVATION {
  polygon-opacity: 0.3;
  polygon-fill: #006d2c;
  line-color: "#7b7b7b";
  line-width: 0.5;
  line-opacity: 0.5;
}

#OBSERVATION {
  [count <= 1000000] { polygon-fill: #006d2c;  }
  [count <= 100000] { polygon-fill: #31a354;  }
  [count <= 10000] { polygon-fill: #74c476;  }
  [count <= 1000] { polygon-fill: #bae4b3;  }
  [count <= 100] { polygon-fill: #edf8e9;  }
}


