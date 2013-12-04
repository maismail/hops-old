package org.apache.hadoop.hdfs.server.blockmanagement;

import se.sics.hop.metadata.persistence.CounterType;
import se.sics.hop.metadata.persistence.FinderType;

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
