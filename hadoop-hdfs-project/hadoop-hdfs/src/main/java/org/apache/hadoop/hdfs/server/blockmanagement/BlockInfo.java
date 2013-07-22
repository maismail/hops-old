/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.persistance.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 * Internal class for block metadata. BlockInfo class maintains for a given
 * block the {@link BlockCollection} it is part of and datanodes where the
 * replicas of the block are stored.
 */
@InterfaceAudience.Private
public class BlockInfo extends Block {
  
  public static final BlockInfo[] EMPTY_ARRAY = {};
  private static final List<IndexedReplica> EMPTY_REPLICAS_ARRAY = Collections.unmodifiableList(new ArrayList<IndexedReplica>());
  
  public static enum Counter implements CounterType<BlockInfo> {
    
    All;
    
    @Override
    public Class getType() {
      return BlockInfo.class;
    }
  }
  
  public static enum Finder implements FinderType<BlockInfo> {
    
    ById, ByInodeId, All, ByStorageId;
    
    @Override
    public Class getType() {
      return BlockInfo.class;
    }
  }
  
  public static enum Order implements Comparator<BlockInfo> {
    
    ByBlockIndex() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if (o1.getBlockIndex() < o2.getBlockIndex()) {
          return -1;
        } else {
          return 1;
        }
      }
    },
    ByGenerationStamp() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if (o1.getGenerationStamp() < o2.getGenerationStamp()) {
          return -1;
        } else {
          return 1;
        }
      }
    };
    
    @Override
    public abstract int compare(BlockInfo o1, BlockInfo o2);
    
    public Comparator acsending() {
      return this;
    }
    
    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }
  private BlockCollection bc;
  private int blockIndex = -1;  
  private long timestamp = 1;
  protected long inodeId = -1;
  
  public BlockInfo(Block blk) {
    super(blk);
    if (blk instanceof BlockInfo) {
      this.bc = ((BlockInfo) blk).bc;
    }
  }

  /**
   * Copy construction. This is used to convert BlockInfoUnderConstruction
   *
   * @param from BlockInfo to copy from.
   */
  protected BlockInfo(BlockInfo from) {
    super(from);
    this.bc = from.bc;
  }
  
  public BlockCollection getBlockCollection() throws PersistanceException {
    if (bc == null) {
      setBlockCollection((BlockCollection) EntityManager.find(INodeFile.Finder.ByPKey, inodeId));
    }
    return bc;
  }
  
  public void setBlockCollection(BlockCollection bc) throws PersistanceException {
    this.bc = bc;
    if (bc != null) {
      setINodeId(bc.getId());      
    } else {
      setINodeIdNoPersistance(-1);
      remove();
    }
  }

  /**
   * Count the number of data-nodes the block belongs to.
   */
  public int numNodes() throws PersistanceException {
    return getReplicas().size();
  }

  //HOP: Mahmoud: limit acces to these methods, package private, only BlockManager and DataNodeDescriptor should have access
  List<IndexedReplica> getReplicas() throws PersistanceException {
    List<IndexedReplica> replicas = (List<IndexedReplica>) EntityManager.findList(IndexedReplica.Finder.ByBlockId, getBlockId());
    if (replicas == null) {
      replicas = EMPTY_REPLICAS_ARRAY;
    } else {
      Collections.sort(replicas, IndexedReplica.Order.ByIndex);
    }
    return replicas;
  }

  /**
   * Adds new replica for this block.
   */
  IndexedReplica addReplica(DatanodeDescriptor dn) throws PersistanceException {
    IndexedReplica replica = new IndexedReplica(getBlockId(), dn.getStorageID(), getReplicas().size());
    add(replica);    
    return replica;
  }

  /**
   * removes a replica of this block related to storageId
   *
   * @param storageId
   * @return
   */
  IndexedReplica removeReplica(DatanodeDescriptor dn) throws PersistanceException {
    List<IndexedReplica> replicas = getReplicas();
    IndexedReplica replica = null;
    int index = -1;
    for (int i = 0; i < replicas.size(); i++) {
      if (replicas.get(i).getStorageId().equals(dn.getStorageID())) {
        index = i;
        break;
      }
    }
    if (index >= 0) {
      replica = replicas.remove(index);
      remove(replica);
      
      for (int i = index; i < replicas.size(); i++) {        
        IndexedReplica r1 = replicas.get(i);
        r1.setIndex(i);
        save(r1);
      }
      
    }
    return replica;
  }
  
  boolean hasReplicaIn(DatanodeDescriptor dn) throws PersistanceException {
    for (IndexedReplica replica : getReplicas()) {
      if (replica.getStorageId().equals(dn.getStorageID())) {
        return true;
      }
    }
    return false;
  }

  /**
   * BlockInfo represents a block that is not being constructed. In order to
   * start modifying the block, the BlockInfo should be converted to
   * {@link BlockInfoUnderConstruction}.
   *
   * @return {@link BlockUCState#COMPLETE}
   */
  public BlockUCState getBlockUCState() {
    return BlockUCState.COMPLETE;
  }

  /**
   * Is this block complete?
   *
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  /**
   * Convert a complete block to an under construction block.
   *
   * @return BlockInfoUnderConstruction - an under construction block.
   */
  public BlockInfoUnderConstruction convertToBlockUnderConstruction(
          BlockUCState s, DatanodeDescriptor[] targets) throws PersistanceException {
    if (isComplete()) {
      return new BlockInfoUnderConstruction(this, s, targets);
    }
    // the block is already under construction
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    return ucBlock;
  }
  
  public long getInodeId() {
    return inodeId;
  }
  
  public void setINodeIdNoPersistance(long id) {
    this.inodeId = id;
  }
  
  public void setINodeId(long id) throws PersistanceException {
    setINodeIdNoPersistance(id);
    save();
  }
  
  public int getBlockIndex() {
    return this.blockIndex;
  }
  
  public void setBlockIndexNoPersistance(int bindex) {
    this.blockIndex = bindex;
  }
  
  public void setBlockIndex(int bindex) throws PersistanceException {
    setBlockIndexNoPersistance(bindex);
    save();
  }
  
  public long getTimestamp() {
    return this.timestamp;
  }
  
  public void setTimestampNoPersistance(long ts) {
    this.timestamp = ts;
  }
  
  public void setTimestamp(long ts) throws PersistanceException {
    setTimestampNoPersistance(ts);
    save();
  }
  
  protected void add(IndexedReplica replica) throws PersistanceException {
    EntityManager.add(replica);
  }
  
  protected void remove(IndexedReplica replica) throws PersistanceException {
    EntityManager.remove(replica);
  }
  
  protected void save(IndexedReplica replica) throws PersistanceException {
    EntityManager.update(replica);
  }
  
  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }
}
