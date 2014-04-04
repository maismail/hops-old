package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;

public abstract class BlockRepairManager extends ConfiguredExecutionResultCallbackCaller implements Stoppable {

  public static enum Result{
    SUCCESS,
    FAILED,
    ABORTED
  }

  public BlockRepairManager(Configuration conf, ExecutionResultCallback<FileStatus, Result> callback) {
    super(conf, callback);
  }

  public abstract void repairSourceBlocks(FileStatus sourceFile, FileStatus parityFile, BlockLocation[] brokenBlocks);
  public abstract void repairParityBlocks(FileStatus sourceFile, FileStatus parityFile, BlockLocation[] brokenBlocks);
}
