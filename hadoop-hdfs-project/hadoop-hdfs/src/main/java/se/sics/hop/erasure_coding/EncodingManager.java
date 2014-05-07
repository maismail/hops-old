package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;

public abstract class EncodingManager extends Configured implements Stoppable {

  public static enum Result{
    SUCCESS,
    FAILED,
    ABORTED
  }

  public EncodingManager(Configuration conf) {
    super(conf);
  }

  public abstract void encodeFile(String codecId, String filePath);
}
