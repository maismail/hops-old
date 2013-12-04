package org.apache.hadoop.hdfs.server.blockmanagement;

import se.sics.hop.metadata.persistence.CounterType;
import se.sics.hop.metadata.persistence.FinderType;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplica extends Replica {

  public static enum Counter implements CounterType<ExcessReplica> {

    All;

    @Override
    public Class getType() {
      return ExcessReplica.class;
    }
  }

  public static enum Finder implements FinderType<ExcessReplica> {

    ByStorageId, ByPKey, ByBlockId;

    @Override
    public Class getType() {
      return ExcessReplica.class;
    }
  }

  public ExcessReplica(String storageId, long blockId) {
    super(storageId, blockId);
  }
}
