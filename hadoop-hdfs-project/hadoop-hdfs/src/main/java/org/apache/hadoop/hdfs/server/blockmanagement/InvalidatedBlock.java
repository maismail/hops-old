package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.server.namenode.persistance.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.FinderType;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlock extends Replica {

  public static enum Counter implements CounterType<InvalidatedBlock> {

    All;

    @Override
    public Class getType() {
      return InvalidatedBlock.class;
    }
  }

  public static enum Finder implements FinderType<InvalidatedBlock> {

    ByBlockId, ByStorageId, ByPrimaryKey, All;

    @Override
    public Class getType() {
      return InvalidatedBlock.class;
    }
  }
  private long generationStamp;
  private long numBytes;

  public InvalidatedBlock(String storageId, long blockId) {
    super(storageId, blockId);
  }

  public InvalidatedBlock(String storageId, long blockId, long generationStamp, long numBytes) {
    super(storageId, blockId);
    this.generationStamp = generationStamp;
    this.numBytes = numBytes;
  }

  /**
   * @return the generationStamp
   */
  public long getGenerationStamp() {
    return generationStamp;
  }

  /**
   * @param generationStamp the generationStamp to set
   */
  public void setGenerationStamp(long generationStamp) {
    this.generationStamp = generationStamp;
  }

  /**
   * @return the numBytes
   */
  public long getNumBytes() {
    return numBytes;
  }

  /**
   * @param numBytes the numBytes to set
   */
  public void setNumBytes(long numBytes) {
    this.numBytes = numBytes;
  }
}
