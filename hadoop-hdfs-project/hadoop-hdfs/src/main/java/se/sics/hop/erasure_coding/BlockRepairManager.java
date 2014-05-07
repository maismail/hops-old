package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;

public abstract class BlockRepairManager extends Configured implements Stoppable {

  public static enum Result{
    SUCCESS,
    FAILED,
    ABORTED
  }

  public BlockRepairManager(Configuration conf) {
    super(conf);
  }

  public abstract void repairSourceBlocks(String codecId, String sourceFile);
  public abstract void repairParityBlocks(String codecId, String sourceFile);
}
