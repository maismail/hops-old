package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.List;

public abstract class EncodingManager extends Configured {

  private final EncodingStatusCallback callback;

  public EncodingManager(Configuration conf, EncodingStatusCallback callback) {
    super(conf);
    this.callback = callback;
  }

  protected EncodingStatusCallback getCallback() {
    return callback;
  }

  public abstract void raidFiles(PolicyInfo info, List<FileStatus> paths)
      throws IOException;
  public abstract int getRunningJobsForPolicy(String policyName);
  public abstract void stop();
}
