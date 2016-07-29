
#occurrence-green {
  dot-width: 2;
  dot-opacity: 1;
  [total <= 10] { dot-fill: #a50f15;  }
  [total > 10][total <= 100] { dot-fill: #de2d26;  }
  [total > 100][total <= 1000] { dot-fill: #fb6a4a;  }
  [total > 1000][total <= 10000] { dot-fill: #fcae91;  }
  [total > 10000][total <= 100000] { dot-fill: #fee5d9;  }
}


#occurrence-blue {
  dot-width: 2;
  dot-opacity: 1;
  [total <= 10] { dot-fill: #08519c;  }
  [total > 10][total <= 100] { dot-fill: #3182bd;  }
  [total > 100][total <= 1000] { dot-fill: #6baed6;  }
  [total > 1000][total <= 10000] { dot-fill: #bdd7e7;  }
  [total > 10000][total <= 100000] { dot-fill: #eff3ff;  }
}

#occurrence-nice {
  dot-width: 2;
  dot-opacity: 1;
  [total <= 10] { dot-fill: #5e0063;  }
  [total > 10][total <= 100] { dot-fill: #a42e61;  }
  [total > 100][total <= 1000] { dot-fill: #d26b63;  }
  [total > 1000][total <= 10000] { dot-fill: #eeab79;  }
  [total > 10000][total <= 100000] { dot-fill: #ffebaa;  }
}

#occurrence {
  dot-width: 2;
  dot-opacity: 1;
  [total <= 5] { dot-fill: #5e0063;  }
  [total > 5][total <= 10] { dot-fill: #851362;  }
  [total > 10][total <= 50] { dot-fill: #a42e61;  }
  [total > 50][total <= 100] { dot-fill: #be4c60;  }
  [total > 100][total <= 500] { dot-fill: #d26b63;  }
  [total > 500][total <= 1000] { dot-fill: #e28b6b;  }
  [total > 1000][total <= 50000] { dot-fill: #eeab79;  }
  [total > 5000][total <= 10000] { dot-fill: #f7cb8e;  }
  [total > 10000][total <= 100000] { dot-fill: #ffebaa;  }
}

#occurrence-purple {
  dot-width: 2;
  dot-opacity: 1;
  [total <= 5] { dot-fill: #352136;  }
  [total > 5][total <= 10] { dot-fill: #4e2e4f;  }
  [total > 10][total <= 50] { dot-fill: #673c68;  }
  [total > 50][total <= 100] { dot-fill: #804981;  }
  [total > 100][total <= 500] { dot-fill: #9a579a;  }
  [total > 500][total <= 1000] { dot-fill: #b364b3;  }
  [total > 1000][total <= 50000] { dot-fill: #cc72cc;  }
  [total > 5000][total <= 10000] { dot-fill: #e57fe5;  }
  [total > 10000][total <= 100000] { dot-fill: #ff8dff;  }
}
