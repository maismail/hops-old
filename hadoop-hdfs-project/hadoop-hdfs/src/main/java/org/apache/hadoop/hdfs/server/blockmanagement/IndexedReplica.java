package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Comparator;
import se.sics.hop.metadata.persistence.FinderType;

/**
 * This class holds the information of one replica of a block in one datanode.
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public class IndexedReplica extends Replica {

  public static enum Finder implements FinderType<IndexedReplica> {

    ByBlockId;

    @Override
    public Class getType() {
      return IndexedReplica.class;
    }
  }
  
   public static enum Order implements Comparator<IndexedReplica> {

    ByIndex() {

      @Override
      public int compare(IndexedReplica o1, IndexedReplica o2) {
        if (o1.getIndex() < o2.getIndex()) {
          return -1;
        } else {
          return 1;
        }
      }
    };
  }
  int index;

  public IndexedReplica(long blockId, String storageId, int index) {
    super(storageId, blockId);
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  @Override
  public String toString() {
    return "sid " + storageId + " bid: " + blockId + " index: " + index;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj); //To change body of generated methods, choose Tools | Templates.
  }
}
