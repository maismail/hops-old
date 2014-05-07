package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import java.util.List;

public abstract class BlockRepairManager extends Configured implements Cancelable<String> {

  public BlockRepairManager(Configuration conf) {
    super(conf);
  }

  public abstract void repairSourceBlocks(String codecId, Path sourceFile);
  public abstract void repairParityBlocks(String codecId, Path sourceFile);
  public abstract List<Report> computeReports();
}
