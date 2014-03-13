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

import se.sics.hop.metadata.hdfs.entity.hop.HopReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import se.sics.hop.Common;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;

/**
 * Internal class for block metadata. BlockInfo class maintains for a given
 * block the {@link BlockCollection} it is part of and datanodes where the
 * replicas of the block are stored.
 */
@InterfaceAudience.Private
public class BlockInfo extends Block {
  
  public static final BlockInfo[] EMPTY_ARRAY = {};
  private static final List<HopIndexedReplica> EMPTY_REPLICAS_ARRAY = Collections.unmodifiableList(new ArrayList<HopIndexedReplica>());
  
  public static enum Counter implements CounterType<BlockInfo> {
    
    All;
    
    @Override
    public Class getType() {
      return BlockInfo.class;
    }
  }
  
  public static enum Finder implements FinderType<BlockInfo> {
    
    ById, ByInodeId, All, ByStorageId, MAX_BLK_INDX;
    
    @Override
    public Class getType() {
      return BlockInfo.class;
    }
  }
  
  public static enum Order implements Comparator<BlockInfo> {
    
    ByBlockIndex() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if(o1.getBlockIndex() == o2.getBlockIndex()){
          throw new IllegalStateException("A file cannot have 2 blocks with the same index. index = "
                  +o1.getBlockIndex()+" blk1_id = "+o1.getBlockId()+" blk2_id = "+o2.getBlockId());
        }
        if (o1.getBlockIndex() < o2.getBlockIndex()) {
          return -1;
        } else {
          return 1;
        }
      }
    },
    ByBlockId() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if(o1.getBlockId() == o2.getBlockId()){
          return 0;
        }
        if (o1.getBlockId() < o2.getBlockId()) {
          return -1;
        } else {
          return 1;
        }
      }
    },
    ByGenerationStamp() {
      @Override
      public int compare(BlockInfo o1, BlockInfo o2) {
        if(o1.getGenerationStamp() == o2.getGenerationStamp()){
          throw new IllegalStateException("A file cannot have 2 blocks with the same generation stamp");
        }
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
      this.blockIndex = ((BlockInfo) blk).blockIndex;
      this.timestamp = ((BlockInfo) blk).timestamp;
      this.inodeId = ((BlockInfo) blk).inodeId;
    }
  }
  
  public BlockInfo(){
    this.bc = null;
  }
  /**
   * Copy construction. This is used to convert BlockInfoUnderConstruction
   *
   * @param from BlockInfo to copy from.
   */
  protected BlockInfo(BlockInfo from) {
    super(from);
    this.bc = from.bc;
    this.blockIndex = from.blockIndex;
    this.timestamp = from.timestamp;
    this.inodeId = from.inodeId;
  }
  
  public BlockCollection getBlockCollection() throws PersistanceException {
    //Every time get block collection is called, get it from DB
    //Why? some times it happens that the inode is deleted and copy 
    //of the block is lying around is some secondary data structure ( not block_info )
    //if we call get block collection op of that copy then it should return null

    BlockCollection bc = (BlockCollection) EntityManager.find(INodeFile.Finder.ByINodeID, inodeId);
    this.bc = bc; 
    if(bc == null){
      this.inodeId = INode.NON_EXISTING_ID;
    }
    return bc;
  }
  
  public void setBlockCollection(BlockCollection bc) throws PersistanceException {
    this.bc = bc;
    if (bc != null) {
      setINodeId(bc.getId());      
    }
//  we removed the block removal from inside INodeFile to BlocksMap 
//    else {
//      setINodeIdNoPersistance(-1);
//      remove();
//    }
  }

  /**
   * Count the number of data-nodes the block belongs to.
   */
  public int numNodes(DatanodeManager datanodeMgr) throws PersistanceException {
    return getReplicas(datanodeMgr).size();
  }

  public DatanodeDescriptor[] getDatanodes(DatanodeManager datanodeMgr) throws PersistanceException{
    List<HopIndexedReplica> replicas = getReplicas(datanodeMgr);
    return getDatanodes(datanodeMgr, replicas);
  }
  
  //HOP: Mahmoud: limit acces to these methods, package private, only BlockManager and DataNodeDescriptor should have access
  List<HopIndexedReplica> getReplicasNoCheck() throws PersistanceException {
    List<HopIndexedReplica> replicas = (List<HopIndexedReplica>) EntityManager.findList(HopIndexedReplica.Finder.ByBlockId, getBlockId());
    if (replicas == null) {
      replicas = EMPTY_REPLICAS_ARRAY;
    } else {
      Collections.sort(replicas, HopIndexedReplica.Order.ByIndex);
    }
    return replicas;
  }

    //HOP: Mahmoud: limit acces to these methods, package private, only BlockManager and DataNodeDescriptor should have access
  List<HopIndexedReplica> getReplicas(DatanodeManager datanodeMgr) throws PersistanceException {
    List<HopIndexedReplica> replicas = getReplicasNoCheck();
    getDatanodes(datanodeMgr, replicas);
    Collections.sort(replicas, HopIndexedReplica.Order.ByIndex);
    return replicas; 
  }

  
  /**
   * Adds new replica for this block.
   */
  HopIndexedReplica addReplica(DatanodeDescriptor dn) throws PersistanceException {
    HopIndexedReplica replica = new HopIndexedReplica(getBlockId(), dn.getSId(),/*FIXME [M]*/ getReplicasNoCheck().size());
    add(replica);    
    return replica;
  }

  public void removeAllReplicas() throws PersistanceException {
    for (HopIndexedReplica replica : getReplicasNoCheck()) {
      remove(replica);
    }
  }
  /**
   * removes a replica of this block related to storageId
   *
   * @param storageId
   * @return
   */
  HopIndexedReplica removeReplica(DatanodeDescriptor dn) throws PersistanceException {
    List<HopIndexedReplica> replicas = getReplicasNoCheck();
    HopIndexedReplica replica = null;
    int index = -1;
    for (int i = 0; i < replicas.size(); i++) {
      if (replicas.get(i).getStorageId() == dn.getSId()) {
        index = i;
        break;
      }
    }
    if (index >= 0) {
      replica = replicas.remove(index);
      remove(replica);
      
      for (int i = index; i < replicas.size(); i++) {        
        HopIndexedReplica r1 = replicas.get(i);
        r1.setIndex(i);
        save(r1);
      }
      
    }
    return replica;
  }
  
  int findDatanode(DatanodeDescriptor dn) throws PersistanceException {
    List<HopIndexedReplica> replicas = getReplicasNoCheck();
    for (int i = 0; i < replicas.size(); i++) {
      if (replicas.get(i).getStorageId() == dn.getSId()) {
        return i;
      }
    }
    return -1;
  }

  boolean hasReplicaIn(DatanodeDescriptor dn) throws PersistanceException {
    for (HopIndexedReplica replica : getReplicasNoCheck()) {
      if (replica.getStorageId() == dn.getSId()) {
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
  
  protected DatanodeDescriptor[] getDatanodes(DatanodeManager datanodeMgr, List<? extends HopReplica> replicas) {
    int numLocations = replicas.size();
    List<DatanodeDescriptor> list = new ArrayList<DatanodeDescriptor>();
    for (int i = numLocations-1; i >= 0 ; i--) {
      DatanodeDescriptor desc = datanodeMgr.getDatanode(replicas.get(i).getStorageId());
      if (desc != null) {
        list.add(desc);
      }else{
        replicas.remove(i);
      }
    }
    DatanodeDescriptor[] locations = new DatanodeDescriptor[list.size()];
    return list.toArray(locations);
  }
    
  protected void add(HopIndexedReplica replica) throws PersistanceException {
    EntityManager.add(replica);
  }
  
  protected void remove(HopIndexedReplica replica) throws PersistanceException {
    EntityManager.remove(replica);
  }
  
  protected void save(HopIndexedReplica replica) throws PersistanceException {
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
  
  @Override
  public String toString(){
   return "ID = "+getBlockId()+"  State = "+getBlockUCState();
  }
  //START_HOP_CODE
  
  public void setBlockId(long bid) throws PersistanceException {
    setBlockIdNoPersistance(bid);
    save();
  }

  public void setNumBytes(long len) throws PersistanceException {
    setNumBytesNoPersistance(len);
    save();
  }

  public void setGenerationStamp(long stamp) throws PersistanceException {
    setGenerationStampNoPersistance(stamp);
    save();
  }

  public void set(long blkid, long len, long genStamp) throws PersistanceException {
    setNoPersistance(blkid, len, genStamp);
    save();
  }
  
  protected void save() throws PersistanceException {
    save(this);
  }

  protected void save(BlockInfo blk) throws PersistanceException {
    EntityManager.update(blk);
  }

  protected void remove() throws PersistanceException {
    remove(this);
  }

  protected void remove(BlockInfo blk) throws PersistanceException {
    EntityManager.remove(blk);
  }
  //END_HOP_CODE


}
