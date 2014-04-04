package se.sics.hop.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public abstract class ConfiguredExecutionResultCallbackCaller extends Configured {

  private final ExecutionResultCallback callback;

  public ConfiguredExecutionResultCallbackCaller(Configuration conf, ExecutionResultCallback callback) {
    super(conf);
    this.callback = callback;
  }

  protected ExecutionResultCallback getCallback() {
    return callback;
  }
}