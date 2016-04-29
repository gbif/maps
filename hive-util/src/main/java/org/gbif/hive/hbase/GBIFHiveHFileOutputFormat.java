package org.gbif.hive.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.hbase.HiveHFileOutputFormat;
import org.apache.hadoop.hive.hbase.PutWritable;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * This class exists because of https://issues.apache.org/jira/browse/HIVE-13539 and will be removed
 * This class It does exactly the same as the HFileOutputFormat in Hive, but selects the task attempt directory differently.
 */
@Deprecated
public class GBIFHiveHFileOutputFormat extends HiveHFileOutputFormat
  implements HiveOutputFormat<ImmutableBytesWritable, KeyValue> {

  public static final String HFILE_FAMILY_PATH = "hfile.family.path";
  static final Log LOG = LogFactory.getLog(GBIFHiveHFileOutputFormat.class.getName());

  private RecordWriter<ImmutableBytesWritable, KeyValue> getFileWriter(TaskAttemptContext tac) throws IOException {
    try {
      return super.getRecordWriter(tac);
    } catch (InterruptedException var3) {
      throw new IOException(var3);
    }
  }

  public static String getFamilyPath(Configuration jc, Properties tableProps) {
    return jc.get("hfile.family.path", tableProps.getProperty("hfile.family.path"));
  }

  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
    final JobConf jc,
    final Path finalOutPath,
    Class<? extends Writable> valueClass,
    boolean isCompressed,
    Properties tableProperties,
    Progressable progressable
  ) throws IOException {
    String hfilePath = getFamilyPath(jc, tableProperties);
    if (hfilePath == null) {
      throw new RuntimeException("Please set hfile.family.path to target location for HFiles");
    } else {
      final Path columnFamilyPath = new Path(hfilePath);
      final String columnFamilyName = columnFamilyPath.getName();
      final byte[] columnFamilyNameBytes = Bytes.toBytes(columnFamilyName);
      Job job = new Job(jc);
      setCompressOutput(job, isCompressed);
      setOutputPath(job, finalOutPath);
      TaskAttemptContext tac = ShimLoader.getHadoopShims().newTaskAttemptContext(job.getConfiguration(), progressable);

      final Path outputdir = FileOutputFormat.getOutputPath(tac);
      final Path taskAttemptOutputdir = FileOutputCommitter.getTaskAttemptPath(tac, outputdir);

      final RecordWriter fileWriter = this.getFileWriter(tac);
      String columnList = tableProperties.getProperty("columns");
      String[] columnArray = columnList.split(",");
      final TreeMap columnMap = new TreeMap(Bytes.BYTES_COMPARATOR);
      int i = 0;
      String[] arr$ = columnArray;
      int len$ = columnArray.length;

      for (int i$ = 0; i$ < len$; ++i$) {
        String columnName = arr$[i$];
        if (i != 0) {
          columnMap.put(Bytes.toBytes(columnName), Integer.valueOf(i));
        }

        ++i;
      }

      return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {

        public void close(boolean abort) throws IOException {
          try {
            fileWriter.close((TaskAttemptContext) null);
            if (!abort) {
              FileSystem ex = outputdir.getFileSystem(jc);
              ex.mkdirs(columnFamilyPath);
              Path srcDir = taskAttemptOutputdir;
              FileStatus[] arr$;
              do {
                arr$ = ex.listStatus(srcDir, FileUtils.STAGING_DIR_PATH_FILTER);
                if (arr$ == null || arr$.length == 0) {
                  throw new IOException("No family directories found in " + srcDir);
                }

                if (arr$.length != 1) {
                  throw new IOException("Multiple family directories found in " + srcDir);
                }

                srcDir = arr$[0].getPath();
              } while (!srcDir.getName().equals(columnFamilyName));

              arr$ = ex.listStatus(srcDir, FileUtils.STAGING_DIR_PATH_FILTER);
              int len$ = arr$.length;

              for (int i$ = 0; i$ < len$; ++i$) {
                FileStatus regionFile = arr$[i$];
                ex.rename(regionFile.getPath(), new Path(columnFamilyPath, regionFile.getPath().getName()));
              }

              ex.delete(taskAttemptOutputdir, true);
              ex.createNewFile(taskAttemptOutputdir);
            }
          } catch (InterruptedException var8) {
            throw new IOException(var8);
          }
        }

        private void writeText(Text text) throws IOException {
          String s = text.toString();
          String[] fields = s.split("\u0001");

          assert fields.length <= columnMap.size() + 1;

          byte[] rowKeyBytes = Bytes.toBytes(fields[0]);
          Iterator i$ = columnMap.entrySet().iterator();

          while (true) {
            byte[] columnNameBytes;
            String val;
            do {
              if (!i$.hasNext()) {
                return;
              }

              Entry entry = (Entry) i$.next();
              columnNameBytes = (byte[]) entry.getKey();
              int iColumn = ((Integer) entry.getValue()).intValue();
              if (iColumn >= fields.length) {
                val = "";
                break;
              }

              val = fields[iColumn];
            } while ("\\N".equals(val));

            byte[] valBytes = Bytes.toBytes(val);
            KeyValue kv = new KeyValue(rowKeyBytes, columnFamilyNameBytes, columnNameBytes, valBytes);

            try {
              fileWriter.write((Object) null, kv);
            } catch (IOException var13) {
              GBIFHiveHFileOutputFormat.LOG.error("Failed while writing row: " + s);
              throw var13;
            } catch (InterruptedException var14) {
              throw new IOException(var14);
            }
          }
        }

        private void writePut(PutWritable put) throws IOException {
          ImmutableBytesWritable row = new ImmutableBytesWritable(put.getPut().getRow());
          NavigableMap cells = put.getPut().getFamilyCellMap();
          Iterator i$ = cells.entrySet().iterator();

          while (i$.hasNext()) {
            Entry entry = (Entry) i$.next();
            Collections.sort((List) entry.getValue(), new CellComparator());
            Iterator i$1 = ((List) entry.getValue()).iterator();

            while (i$1.hasNext()) {
              Cell c = (Cell) i$1.next();

              try {
                fileWriter.write(row, KeyValueUtil.copyToNewKeyValue(c));
              } catch (InterruptedException var9) {
                throw (InterruptedIOException) (new InterruptedIOException()).initCause(var9);
              }
            }
          }

        }

        public void write(Writable w) throws IOException {
          if (w instanceof Text) {
            this.writeText((Text) w);
          } else {
            if (!(w instanceof PutWritable)) {
              throw new IOException("Unexpected writable " + w);
            }

            this.writePut((PutWritable) w);
          }

        }
      };
    }
  }

  public void checkOutputSpecs(FileSystem ignored, JobConf jc) throws IOException {
    Job job = new Job(jc);
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
    this.checkOutputSpecs(jobContext);
  }

  public org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(
    FileSystem ignored,
    JobConf job,
    String name,
    Progressable progress
  ) throws IOException {
    throw new NotImplementedException("This will not be invoked");
  }
}
