package org.apache.hadoop.hbase.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience.Public;
import org.apache.hadoop.hbase.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.AbstractHFileWriter;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFile.WriterBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.fs.HFileSystem;

/**
 * Patched version of HFileOutputFormat2 which cherry picks from the result of HBASE-12596.
 * The additions are only to include a hint to HDFS of which host to put the HFiles on.  By doing this, we have a far
 * higher chance of achieving 1.0 data locality on HBase when the HFiles are loaded in.
 * <p/>
 * This is not needed if running HBase 1.3.x+ but at the time of writing GBIF run CDH 5.4.10 (HBase 1.0.x).
 */
@Public
@Evolving
public class PatchedHFileOutputFormat2 extends FileOutputFormat<ImmutableBytesWritable, Cell> {
  static Log LOG = LogFactory.getLog(PatchedHFileOutputFormat2.class);
  private static final String COMPRESSION_FAMILIES_CONF_KEY = "hbase.hfileoutputformat.families.compression";
  private static final String BLOOM_TYPE_FAMILIES_CONF_KEY = "hbase.hfileoutputformat.families.bloomtype";
  private static final String BLOCK_SIZE_FAMILIES_CONF_KEY = "hbase.mapreduce.hfileoutputformat.blocksize";
  private static final String DATABLOCK_ENCODING_FAMILIES_CONF_KEY = "hbase.mapreduce.hfileoutputformat.families.datablock.encoding";
  public static final String DATABLOCK_ENCODING_OVERRIDE_CONF_KEY = "hbase.mapreduce.hfileoutputformat.datablock.encoding";

  /**
   * Keep locality while generating HFiles for bulkload. See HBASE-12596
   */
  public static final String LOCALITY_SENSITIVE_CONF_KEY = "hbase.bulkload.locality.sensitive.enabled";
  private static final boolean DEFAULT_LOCALITY_SENSITIVE = true;
  private static final String OUTPUT_TABLE_NAME_CONF_KEY = "hbase.mapreduce.hfileoutputformat.table.name";


  public PatchedHFileOutputFormat2() {
  }

  public RecordWriter<ImmutableBytesWritable, Cell> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return createRecordWriter(context);
  }

  static <V extends Cell> RecordWriter<ImmutableBytesWritable, V> createRecordWriter(final TaskAttemptContext context) throws IOException {
    Path outputPath = FileOutputFormat.getOutputPath(context);
    final Path outputdir = (new FileOutputCommitter(outputPath, context)).getWorkPath();
    final Configuration conf = context.getConfiguration();
    final FileSystem fs = outputdir.getFileSystem(conf);
    final long maxsize = conf.getLong("hbase.hregion.max.filesize", 10737418240L);
    String defaultCompressionStr = conf.get("hfile.compression", Algorithm.NONE.getName());
    final Algorithm defaultCompression = AbstractHFileWriter.compressionByName(defaultCompressionStr);
    final boolean compactionExclude = conf.getBoolean("hbase.mapreduce.hfileoutputformat.compaction.exclude", false);
    final Map compressionMap = createFamilyCompressionMap(conf);
    final Map bloomTypeMap = createFamilyBloomTypeMap(conf);
    final Map blockSizeMap = createFamilyBlockSizeMap(conf);
    String dataBlockEncodingStr = conf.get("hbase.mapreduce.hfileoutputformat.datablock.encoding");
    final Map datablockEncodingMap = createFamilyDataBlockEncodingMap(conf);
    final DataBlockEncoding overriddenEncoding;
    if(dataBlockEncodingStr != null) {
      overriddenEncoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
    } else {
      overriddenEncoding = null;
    }

    return new RecordWriter<ImmutableBytesWritable, V>() {
      private final Map<byte[], PatchedHFileOutputFormat2.WriterLength> writers;
      private byte[] previousRow;
      private final byte[] now;
      private boolean rollRequested;

      {
        this.writers = new TreeMap(Bytes.BYTES_COMPARATOR);
        this.previousRow = HConstants.EMPTY_BYTE_ARRAY;
        this.now = Bytes.toBytes(System.currentTimeMillis());
        this.rollRequested = false;
      }

      @Override
      public void write(ImmutableBytesWritable row, V cell) throws IOException {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        if(row == null && kv == null) {
          this.rollWriters();
        } else {
          byte[] rowKey = CellUtil.cloneRow(kv);
          long length = (long)kv.getLength();
          byte[] family = CellUtil.cloneFamily(kv);
          PatchedHFileOutputFormat2.WriterLength wl = (PatchedHFileOutputFormat2.WriterLength)this.writers.get(family);
          if(wl == null) {
            fs.mkdirs(new Path(outputdir, Bytes.toString(family)));
          }

          if(wl != null && wl.written + length >= maxsize) {
            this.rollRequested = true;
          }

          if(this.rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
            this.rollWriters();
          }

          // create a new WAL writer, if necessary
          if(wl == null || wl.writer == null) {
            if (conf.getBoolean(LOCALITY_SENSITIVE_CONF_KEY, DEFAULT_LOCALITY_SENSITIVE)) {
              HRegionLocation loc = null;
              String tableName = conf.get(OUTPUT_TABLE_NAME_CONF_KEY);

              try (Connection connection = ConnectionFactory.createConnection(conf);
                   RegionLocator locator =
                     connection.getRegionLocator(TableName.valueOf(tableName))) {
                loc = locator.getRegionLocation(rowKey);
              } catch (Throwable e) {
                LOG.warn("there's something wrong when locating rowkey: " +
                         Bytes.toString(rowKey), e);
                loc = null;
              }

              if (null == loc) {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("failed to get region location, so use default writer: "
                            + Bytes.toString(rowKey));
                }
                wl = getNewWriter(family, conf, null);
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("first rowkey: [" + Bytes.toString(rowKey) + "]");
                }
                InetSocketAddress initialIsa = new InetSocketAddress(loc.getHostname(), loc.getPort());
                if (initialIsa.isUnresolved()) {
                  if (LOG.isTraceEnabled()) {
                    LOG.trace("failed to resolve bind address: " + loc.getHostname() + ":"
                              + loc.getPort() + ", so use default writer");
                  }
                  wl = getNewWriter(family, conf, null);
                } else {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("use favored nodes writer: " + initialIsa.getHostString());
                  }
                  wl = getNewWriter(family, conf, new InetSocketAddress[] { initialIsa });
                }
              }
            } else {
              wl = getNewWriter(family, conf, null);
            }
          }

          kv.updateLatestStamp(this.now);
          wl.writer.append(kv);
          wl.written += length;
          this.previousRow = rowKey;
        }
      }

      private void rollWriters() throws IOException {
        PatchedHFileOutputFormat2.WriterLength wl;
        for(Iterator i$ = this.writers.values().iterator(); i$.hasNext(); wl.written = 0L) {
          wl = (PatchedHFileOutputFormat2.WriterLength)i$.next();
          if(wl.writer != null) {
            PatchedHFileOutputFormat2.LOG.info("Writer=" + wl.writer.getPath() + (wl.written == 0L?"": ", wrote=" + wl.written));
            this.close(wl.writer);
          }

          wl.writer = null;
        }

        this.rollRequested = false;
      }

      @SuppressWarnings(
        value = {"BX_UNBOXING_IMMEDIATELY_REBOXED"},
        justification = "Not important"
      )
      private PatchedHFileOutputFormat2.WriterLength getNewWriter(byte[] family, Configuration confx, InetSocketAddress[] favoredNodes) throws IOException {
        PatchedHFileOutputFormat2.WriterLength wl = new PatchedHFileOutputFormat2.WriterLength();
        Path familydir = new Path(outputdir, Bytes.toString(family));
        Algorithm compression = (Algorithm)compressionMap.get(family);
        compression = compression == null?defaultCompression:compression;
        BloomType bloomType = (BloomType)bloomTypeMap.get(family);
        bloomType = bloomType == null?BloomType.NONE:bloomType;
        Integer blockSize = (Integer)blockSizeMap.get(family);
        blockSize = Integer.valueOf(blockSize == null?65536:blockSize.intValue());
        DataBlockEncoding encoding = overriddenEncoding;
        encoding = encoding == null?(DataBlockEncoding)datablockEncodingMap.get(family):encoding;
        encoding = encoding == null?DataBlockEncoding.NONE:encoding;
        Configuration tempConf = new Configuration(confx);
        tempConf.setFloat("hfile.block.cache.size", 0.0F);
        HFileContextBuilder contextBuilder = (new HFileContextBuilder()).withCompression(compression).withChecksumType(HStore.getChecksumType(confx)).withBytesPerCheckSum(HStore.getBytesPerChecksum(confx)).withBlockSize(blockSize.intValue());
        contextBuilder.withDataBlockEncoding(encoding);
        HFileContext hFileContext = contextBuilder.build();

        // original behaviour
        if (null == favoredNodes) {
          wl.writer = (new WriterBuilder(confx, new CacheConfig(tempConf), fs)).withOutputDir(familydir).withBloomType(bloomType).withComparator(KeyValue.COMPARATOR).withFileContext(hFileContext).build();
          this.writers.put(family, wl);
          return wl;
        } else {
          wl.writer = (new WriterBuilder(confx, new CacheConfig(tempConf), new HFileSystem(fs)))
            .withOutputDir(familydir).withBloomType(bloomType)
            .withComparator(KeyValue.COMPARATOR).withFileContext(hFileContext)
            .withFavoredNodes(favoredNodes)
            .build();
          this.writers.put(family, wl);
          return wl;

        }
      }

      private void close(Writer w) throws IOException {
        if(w != null) {
          w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
          w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY, Bytes.toBytes(context.getTaskAttemptID().toString()));
          w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
          w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY, Bytes.toBytes(compactionExclude));
          w.appendTrackedTimestampsToMetadata();
          w.close();
        }

      }

      public void close(TaskAttemptContext c) throws IOException, InterruptedException {
        Iterator i$ = this.writers.values().iterator();

        while(i$.hasNext()) {
          PatchedHFileOutputFormat2.WriterLength wl = (PatchedHFileOutputFormat2.WriterLength)i$.next();
          this.close(wl.writer);
        }

      }
    };
  }

  private static List<ImmutableBytesWritable> getRegionStartKeys(RegionLocator table) throws IOException {
    byte[][] byteKeys = table.getStartKeys();
    ArrayList ret = new ArrayList(byteKeys.length);
    byte[][] arr$ = byteKeys;
    int len$ = byteKeys.length;

    for(int i$ = 0; i$ < len$; ++i$) {
      byte[] byteKey = arr$[i$];
      ret.add(new ImmutableBytesWritable(byteKey));
    }

    return ret;
  }

  private static void writePartitions(Configuration conf, Path partitionsPath, List<ImmutableBytesWritable> startKeys) throws IOException {
    LOG.info("Writing partition information to " + partitionsPath);
    if(startKeys.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    } else {
      TreeSet sorted = new TreeSet(startKeys);
      ImmutableBytesWritable first = (ImmutableBytesWritable)sorted.first();
      if(!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
        throw new IllegalArgumentException("First region of table should have empty start key. Instead has: " + Bytes.toStringBinary(first.get()));
      } else {
        sorted.remove(first);
        FileSystem fs = partitionsPath.getFileSystem(conf);
        org.apache.hadoop.io.SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, partitionsPath, ImmutableBytesWritable.class, NullWritable.class);

        try {
          Iterator i$ = sorted.iterator();

          while(i$.hasNext()) {
            ImmutableBytesWritable startKey = (ImmutableBytesWritable)i$.next();
            writer.append(startKey, NullWritable.get());
          }
        } finally {
          writer.close();
        }

      }
    }
  }

  /** @deprecated */
  @Deprecated
  public static void configureIncrementalLoad(Job job, HTable table) throws IOException {
    configureIncrementalLoad(job, table, table);
  }

  public static void configureIncrementalLoad(Job job, Table table, RegionLocator regionLocator) throws IOException {
    configureIncrementalLoad(job, table, regionLocator, PatchedHFileOutputFormat2.class);
  }

  static void configureIncrementalLoad(Job job, Table table, RegionLocator regionLocator, Class<? extends OutputFormat<?, ?>> cls) throws IOException {
    Configuration conf = job.getConfiguration();
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);
    job.setOutputFormatClass(cls);
    if(KeyValue.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(KeyValueSortReducer.class);
    } else if(Put.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(PutSortReducer.class);
    } else if(Text.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(TextSortReducer.class);
    } else {
      LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
    }

    conf.setStrings("io.serializations", new String[]{conf.get("io.serializations"), MutationSerialization.class.getName(), ResultSerialization.class.getName(), KeyValueSerialization.class.getName()});
    if (conf.getBoolean(LOCALITY_SENSITIVE_CONF_KEY, DEFAULT_LOCALITY_SENSITIVE)) {
      // record this table name for creating writer by favored nodes
      LOG.info("bulkload locality sensitive enabled");
      conf.set(OUTPUT_TABLE_NAME_CONF_KEY, regionLocator.getName().getNameAsString());
    }

    LOG.info("Looking up current regions for table " + table.getName());
    List startKeys = getRegionStartKeys(regionLocator);
    LOG.info("Configuring " + startKeys.size() + " reduce partitions " + "to match current region count");
    job.setNumReduceTasks(startKeys.size());
    configurePartitioner(job, startKeys);
    configureCompression(table, conf);
    configureBloomType(table, conf);
    configureBlockSize(table, conf);
    configureDataBlockEncoding(table, conf);
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.initCredentials(job);
    LOG.info("Incremental table " + table.getName() + " output configured.");
  }

  public static void configureIncrementalLoadMap(Job job, Table table) throws IOException {
    Configuration conf = job.getConfiguration();
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);
    job.setOutputFormatClass(PatchedHFileOutputFormat2.class);
    configureCompression(table, conf);
    configureBloomType(table, conf);
    configureBlockSize(table, conf);
    configureDataBlockEncoding(table, conf);
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.initCredentials(job);
    LOG.info("Incremental table " + table.getName() + " output configured.");
  }

  @VisibleForTesting
  static Map<byte[], Algorithm> createFamilyCompressionMap(Configuration conf) {
    Map stringMap = createFamilyConfValueMap(conf, "hbase.hfileoutputformat.families.compression");
    TreeMap compressionMap = new TreeMap(Bytes.BYTES_COMPARATOR);
    Iterator i$ = stringMap.entrySet().iterator();

    while(i$.hasNext()) {
      Entry e = (Entry)i$.next();
      Algorithm algorithm = AbstractHFileWriter.compressionByName((String)e.getValue());
      compressionMap.put(e.getKey(), algorithm);
    }

    return compressionMap;
  }

  @VisibleForTesting
  static Map<byte[], BloomType> createFamilyBloomTypeMap(Configuration conf) {
    Map stringMap = createFamilyConfValueMap(conf, "hbase.hfileoutputformat.families.bloomtype");
    TreeMap bloomTypeMap = new TreeMap(Bytes.BYTES_COMPARATOR);
    Iterator i$ = stringMap.entrySet().iterator();

    while(i$.hasNext()) {
      Entry e = (Entry)i$.next();
      BloomType bloomType = BloomType.valueOf((String)e.getValue());
      bloomTypeMap.put(e.getKey(), bloomType);
    }

    return bloomTypeMap;
  }

  @VisibleForTesting
  static Map<byte[], Integer> createFamilyBlockSizeMap(Configuration conf) {
    Map stringMap = createFamilyConfValueMap(conf, "hbase.mapreduce.hfileoutputformat.blocksize");
    TreeMap blockSizeMap = new TreeMap(Bytes.BYTES_COMPARATOR);
    Iterator i$ = stringMap.entrySet().iterator();

    while(i$.hasNext()) {
      Entry e = (Entry)i$.next();
      Integer blockSize = Integer.valueOf(Integer.parseInt((String)e.getValue()));
      blockSizeMap.put(e.getKey(), blockSize);
    }

    return blockSizeMap;
  }

  @VisibleForTesting
  static Map<byte[], DataBlockEncoding> createFamilyDataBlockEncodingMap(Configuration conf) {
    Map stringMap = createFamilyConfValueMap(conf, "hbase.mapreduce.hfileoutputformat.families.datablock.encoding");
    TreeMap encoderMap = new TreeMap(Bytes.BYTES_COMPARATOR);
    Iterator i$ = stringMap.entrySet().iterator();

    while(i$.hasNext()) {
      Entry e = (Entry)i$.next();
      encoderMap.put(e.getKey(), DataBlockEncoding.valueOf((String)e.getValue()));
    }

    return encoderMap;
  }

  private static Map<byte[], String> createFamilyConfValueMap(Configuration conf, String confName) {
    TreeMap confValMap = new TreeMap(Bytes.BYTES_COMPARATOR);
    String confVal = conf.get(confName, "");
    String[] arr$ = confVal.split("&");
    int len$ = arr$.length;

    for(int i$ = 0; i$ < len$; ++i$) {
      String familyConf = arr$[i$];
      String[] familySplit = familyConf.split("=");
      if(familySplit.length == 2) {
        try {
          confValMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(), URLDecoder.decode(familySplit[1], "UTF-8"));
        } catch (UnsupportedEncodingException var10) {
          throw new AssertionError(var10);
        }
      }
    }

    return confValMap;
  }

  static void configurePartitioner(Job job, List<ImmutableBytesWritable> splitPoints) throws IOException {
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path partitionsPath = new Path(conf.get("hadoop.tmp.dir"), "partitions_" + UUID.randomUUID());
    fs.makeQualified(partitionsPath);
    writePartitions(conf, partitionsPath, splitPoints);
    fs.deleteOnExit(partitionsPath);
    job.setPartitionerClass(TotalOrderPartitioner.class);
    TotalOrderPartitioner.setPartitionFile(conf, partitionsPath);
  }

  @SuppressWarnings({"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"})
  @VisibleForTesting
  static void configureCompression(Table table, Configuration conf) throws IOException {
    StringBuilder compressionConfigValue = new StringBuilder();
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if(tableDescriptor != null) {
      Collection families = tableDescriptor.getFamilies();
      int i = 0;
      Iterator i$ = families.iterator();

      while(i$.hasNext()) {
        HColumnDescriptor familyDescriptor = (HColumnDescriptor)i$.next();
        if(i++ > 0) {
          compressionConfigValue.append('&');
        }

        compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
        compressionConfigValue.append('=');
        compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression().getName(), "UTF-8"));
      }

      conf.set("hbase.hfileoutputformat.families.compression", compressionConfigValue.toString());
    }
  }

  @VisibleForTesting
  static void configureBlockSize(Table table, Configuration conf) throws IOException {
    StringBuilder blockSizeConfigValue = new StringBuilder();
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if(tableDescriptor != null) {
      Collection families = tableDescriptor.getFamilies();
      int i = 0;
      Iterator i$ = families.iterator();

      while(i$.hasNext()) {
        HColumnDescriptor familyDescriptor = (HColumnDescriptor)i$.next();
        if(i++ > 0) {
          blockSizeConfigValue.append('&');
        }

        blockSizeConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
        blockSizeConfigValue.append('=');
        blockSizeConfigValue.append(URLEncoder.encode(String.valueOf(familyDescriptor.getBlocksize()), "UTF-8"));
      }

      conf.set("hbase.mapreduce.hfileoutputformat.blocksize", blockSizeConfigValue.toString());
    }
  }

  @VisibleForTesting
  static void configureBloomType(Table table, Configuration conf) throws IOException {
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if(tableDescriptor != null) {
      StringBuilder bloomTypeConfigValue = new StringBuilder();
      Collection families = tableDescriptor.getFamilies();
      int i = 0;

      String bloomType;
      for(Iterator i$ = families.iterator(); i$.hasNext(); bloomTypeConfigValue.append(URLEncoder.encode(bloomType, "UTF-8"))) {
        HColumnDescriptor familyDescriptor = (HColumnDescriptor)i$.next();
        if(i++ > 0) {
          bloomTypeConfigValue.append('&');
        }

        bloomTypeConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
        bloomTypeConfigValue.append('=');
        bloomType = familyDescriptor.getBloomFilterType().toString();
        if(bloomType == null) {
          bloomType = HColumnDescriptor.DEFAULT_BLOOMFILTER;
        }
      }

      conf.set("hbase.hfileoutputformat.families.bloomtype", bloomTypeConfigValue.toString());
    }
  }

  @VisibleForTesting
  static void configureDataBlockEncoding(Table table, Configuration conf) throws IOException {
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if(tableDescriptor != null) {
      StringBuilder dataBlockEncodingConfigValue = new StringBuilder();
      Collection families = tableDescriptor.getFamilies();
      int i = 0;

      DataBlockEncoding encoding;
      for(Iterator i$ = families.iterator(); i$.hasNext(); dataBlockEncodingConfigValue.append(URLEncoder.encode(encoding.toString(), "UTF-8"))) {
        HColumnDescriptor familyDescriptor = (HColumnDescriptor)i$.next();
        if(i++ > 0) {
          dataBlockEncodingConfigValue.append('&');
        }

        dataBlockEncodingConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
        dataBlockEncodingConfigValue.append('=');
        encoding = familyDescriptor.getDataBlockEncoding();
        if(encoding == null) {
          encoding = DataBlockEncoding.NONE;
        }
      }

      conf.set("hbase.mapreduce.hfileoutputformat.families.datablock.encoding", dataBlockEncodingConfigValue.toString());
    }
  }

  static class WriterLength {
    long written = 0L;
    Writer writer = null;

    WriterLength() {
    }
  }
}
