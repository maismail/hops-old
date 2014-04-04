package se.sics.hop.erasure_coding;

import org.apache.hadoop.fs.FileStatus;

public interface ExecutionResultCallback<IDENTIFIER, RESULT> {

  public void reportExecutionResult(IDENTIFIER identifier, RESULT result);
}
