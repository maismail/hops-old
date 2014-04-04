package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;

public abstract class EncodingManager extends ConfiguredExecutionResultCallbackCaller implements Stoppable {

  public static enum Result{
    SUCCESS,
    FAILED,
    ABORTED
  }

  public EncodingManager(Configuration conf, ExecutionResultCallback<FileStatus, Result> callback) {
    super(conf, callback);
  }

  public abstract void encodeFile(Codec codec, FileStatus sourceFile, FileStatus parityFile);
}
