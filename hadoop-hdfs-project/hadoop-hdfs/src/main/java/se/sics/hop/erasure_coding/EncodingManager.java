package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import java.util.List;

public abstract class EncodingManager extends Configured implements Cancelable<String> {

  public EncodingManager(Configuration conf) {
    super(conf);
  }

  public abstract void encodeFile(EncodingPolicy policy, Path sourceFile);
  public abstract List<Report> computeReports();
}
