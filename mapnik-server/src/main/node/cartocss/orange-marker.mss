/*
 * A green style that paints the center of the polygon with a circle with the circle adjusting size depending on the
 * count.
 */

#occurrence {
  marker-fill: #FF6347;
  marker-allow-overlap: true;
  marker-opacity: 0.8;
  line-color: "#ff0000";
  line-width: 0;
  marker-line-width: 0;
  line-opacity: 0.5;
}

#occurrence {
  [total <= 10] { marker-width: 4;  }
  [total > 10][total <= 100] { marker-width: 8;  }
  [total > 100][total <= 1000] { marker-width: 12;  }
  [total > 1000][total <= 10000] { marker-width: 14;  }
  [total > 10000][total <= 100000] { marker-width: 16;  }
  [total > 100000] { marker-width: 25;  }
}
