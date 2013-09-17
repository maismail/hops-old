package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class LockUpgradeException extends StorageException {

  public LockUpgradeException(String message) {
    super(message);
  }
  
  public LockUpgradeException(Throwable ex)
  {
    super(ex);
  }
}
