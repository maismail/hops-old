package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.server.namenode.persistance.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.FinderType;

/**
 *
 * @author jude
 */
public class CorruptReplica extends Replica {

  public static enum Counter implements CounterType<CorruptReplica> {

    All;

    @Override
    public Class getType() {
      return CorruptReplica.class;
    }
  }

  public static enum Finder implements FinderType<CorruptReplica> {

    All, ByBlockId, ByPk;

    @Override
    public Class getType() {
      return CorruptReplica.class;
    }
  }

  public CorruptReplica(long blockId, String storageId) {
    super(storageId, blockId);
  }
//  public String persistanceKey() {
//    return blockId + storageId;
//  }
}
