package se.sics.hop.erasure_coding;

import org.apache.hadoop.fs.FileStatus;

public interface EncodingStatusCallback {

  public static enum Status{
    SUCCESS,
    FAILED,
    ABORTED
  }

  public void reportEncodingStatus(FileStatus file, Status status);
}
