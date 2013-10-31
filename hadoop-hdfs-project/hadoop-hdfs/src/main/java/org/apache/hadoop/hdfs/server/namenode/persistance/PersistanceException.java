package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class PersistanceException extends IOException{

  public PersistanceException() {
  }

  public PersistanceException(String message) {
    super(message);
  }

  public PersistanceException(Throwable cause) {
    super(cause);
  }

  public PersistanceException(String message, Throwable cause) {
    super(message, cause);
  }
  
}
