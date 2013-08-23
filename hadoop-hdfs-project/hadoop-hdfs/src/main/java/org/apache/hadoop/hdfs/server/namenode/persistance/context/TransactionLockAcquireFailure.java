package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author salman niazi <salman@sics.se>
 */
public class TransactionLockAcquireFailure extends StorageException {

  public TransactionLockAcquireFailure(String msg) {
    super(msg);
  }
  
}
