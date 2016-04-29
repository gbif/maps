package org.gbif.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.HBaseStorageHandler;
import org.apache.hadoop.hive.hbase.HiveHBaseTableOutputFormat;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * This class exists only because of HIVE-13539.
 * This class can be removed when HIVE-13539 is included in the distribution.
 */
@Deprecated
public class GBIFHBaseStorageHandler extends HBaseStorageHandler {

  private Configuration jobConf;

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return isHBaseGenerateHFiles(this.jobConf) ? GBIFHiveHFileOutputFormat.class : HiveHBaseTableOutputFormat.class;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.jobConf = conf;  // required for the getOutputFormatClass() only
  }
}
