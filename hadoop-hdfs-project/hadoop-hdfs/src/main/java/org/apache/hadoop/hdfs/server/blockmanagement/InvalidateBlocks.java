/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.StorageFactory;

/**
 * Keeps a Collection for every named machine containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
class InvalidateBlocks {
  
  private final DatanodeManager datanodeManager;

  InvalidateBlocks(final DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  /** @return the number of blocks to be invalidated . */
  long numBlocks() throws IOException {
    return (Integer) new LightWeightRequestHandler(HDFSOperationType.GET_NUM_INVALIDATED_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /**
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   * @param storageID the storage to check
   * @param the block to look for
   * 
   */
  boolean contains(final String storageID, final BlockInfo block) throws PersistanceException {
    HopInvalidatedBlock blkFound = findBlock(block.getBlockId(), datanodeManager.getDatanode(storageID).getSId(), block.getInodeId());
    if (blkFound == null) {
      return false;
    }
    return blkFound.getGenerationStamp() == block.getGenerationStamp();
  }

  /**
   * Add a block to the block collection
   * which will be invalidated on the specified datanode.
   */
  void add(final BlockInfo block, final DatanodeInfo datanode,
      final boolean log) throws PersistanceException {
    HopInvalidatedBlock invBlk = new HopInvalidatedBlock(datanodeManager.getDatanode(datanode.getStorageID()).getSId(), block.getBlockId(), block.getGenerationStamp(), block.getNumBytes(),block.getInodeId());
    if (add(invBlk)) {
      if (log) {
        NameNode.blockStateChangeLog.info("BLOCK* " + getClass().getSimpleName()
                + ": add " + block + " to " + datanode);
      }
    }
  }

  /** Remove a storage from the invalidatesSet */
  void remove(final String storageID) throws IOException {
    List<HopInvalidatedBlock> invBlocks = findInvBlocksbyStorageId(datanodeManager.getDatanode(storageID).getSId());
    if(invBlocks != null){
      for(HopInvalidatedBlock invBlk : invBlocks){
        if(invBlk != null){
          removeInvBlockTx(invBlk);
        }
      }
    }
  }

  /** Remove the block from the specified storage. */
  void remove(final String storageID, final BlockInfo block) throws PersistanceException {
    removeInvalidatedBlockFromDB(new HopInvalidatedBlock(datanodeManager.getDatanode(storageID).getSId(), block.getBlockId(), block.getGenerationStamp(), block.getNumBytes(),block.getInodeId()));
  }

  /** Print the contents to out. */
  void dump(final PrintWriter out) throws PersistanceException {
    List<HopInvalidatedBlock> invBlocks = findAllInvalidatedBlocks();
    HashSet<Integer> storageIds = new HashSet<Integer>();
    for (HopInvalidatedBlock ib : invBlocks) {
      storageIds.add(ib.getStorageId());
    }
    final int size = storageIds.size();
    out.println("Metasave: Blocks " + invBlocks.size()
            + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }

    for (Integer sId : storageIds) {
      HashSet<HopInvalidatedBlock> invSet = new HashSet<HopInvalidatedBlock>();
      for (HopInvalidatedBlock ib : invBlocks) {
        if (ib.getStorageId() == sId) {
          invSet.add(ib);
        }
      }
      if (invBlocks.size() > 0) {
        out.println(datanodeManager.getDatanode(sId).getName() + invBlocks);
      }
    }
  }

  /** @return a list of the storage IDs. */
  List<String> getStorageIDs() throws IOException {
    LightWeightRequestHandler getAllInvBlocksHandler = new LightWeightRequestHandler(HDFSOperationType.GET_ALL_INV_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        return da.findAllInvalidatedBlocks();
      }
    };
    List<HopInvalidatedBlock> invBlocks = (List<HopInvalidatedBlock>) getAllInvBlocksHandler.handle();
    HashSet<String> storageIds = new HashSet<String>();
    if (invBlocks != null) {
      for (HopInvalidatedBlock ib : invBlocks) {
        storageIds.add(datanodeManager.getDatanode(ib.getStorageId()).getStorageID());
      }
    }
    return new ArrayList<String>(storageIds);
  }

  /** Invalidate work for the storage. */
  int invalidateWork(final String storageId) throws IOException {
    final DatanodeDescriptor dn = datanodeManager.getDatanode(storageId);
    if (dn == null) {
      remove(storageId);
      return 0;
    }
    final List<Block> toInvalidate = invalidateWork(dn.getSId(), dn);
    if (toInvalidate == null) {
      return 0;
    }

    if (NameNode.stateChangeLog.isInfoEnabled()) {
      NameNode.stateChangeLog.info("BLOCK* " + getClass().getSimpleName()
          + ": ask " + dn + " to delete " + toInvalidate);
    }
    return toInvalidate.size();
  }

  private List<Block> invalidateWork(
      final int storageId, final DatanodeDescriptor dn) throws IOException {
    final List<HopInvalidatedBlock> invBlocks = findInvBlocksbyStorageId(storageId);
    if (invBlocks == null || invBlocks.isEmpty()) {
      return null;
    }
    // # blocks that can be sent in one message is limited
    final int limit = datanodeManager.blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<Block>(limit);
    final Iterator<HopInvalidatedBlock> it = invBlocks.iterator();
    for (int count = 0; count < limit && it.hasNext(); count++) {
      HopInvalidatedBlock invBlock = it.next();
      toInvalidate.add(new Block(invBlock.getBlockId(),
              invBlock.getNumBytes(), invBlock.getGenerationStamp()));
      removeInvBlockTx(invBlock);
    }
    dn.addBlocksToBeInvalidated(toInvalidate);
    return toInvalidate;
  }
  
  void clear() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DEL_ALL_INV_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }
  
  
  void add(final Collection<Block> blocks, final DatanodeDescriptor dn) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.ADD_INV_BLOCKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        List<HopInvalidatedBlock> invblks = new ArrayList<HopInvalidatedBlock>();
        for (Block blk : blocks) {
          invblks.add(new HopInvalidatedBlock(dn.getSId(), blk.getBlockId(), blk.getGenerationStamp(), blk.getNumBytes(), INode.NON_EXISTING_ID));
        }
        da.prepare(Collections.EMPTY_LIST, invblks, Collections.EMPTY_LIST);
        return null;
      }
    }.handle();
  }

  private boolean add(HopInvalidatedBlock invBlk) throws PersistanceException {
    HopInvalidatedBlock found = findBlock(invBlk.getBlockId(), invBlk.getStorageId(),invBlk.getInodeId());
    if (found == null) {
      addInvalidatedBlockToDB(invBlk);
      return true;
    }
    return false;
  }
  
  private List<HopInvalidatedBlock> findInvBlocksbyStorageId(final int sid) throws IOException {
    return (List<HopInvalidatedBlock>) new LightWeightRequestHandler(HDFSOperationType.GET_INV_BLKS_BY_STORAGEID) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        return da.findInvalidatedBlockByStorageId(sid);
      }
    }.handle();
  }

  private void removeInvBlockTx(final HopInvalidatedBlock ib) throws IOException {    
     new LightWeightRequestHandler(HDFSOperationType.RM_INV_BLK) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
       InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
       da.remove(ib);
       return null;
      }
    }.handle();
  }
  
  private HopInvalidatedBlock findBlock(long blkId, int storageID, int inodeId) throws PersistanceException {
    return (HopInvalidatedBlock) EntityManager.find(HopInvalidatedBlock.Finder.ByPK, blkId, storageID, inodeId);
  }
  
  private void addInvalidatedBlockToDB(HopInvalidatedBlock invBlk) throws PersistanceException {
    EntityManager.add(invBlk);
  }
  
  private void removeInvalidatedBlockFromDB(HopInvalidatedBlock invBlk) throws PersistanceException {
    EntityManager.remove(invBlk);
  }
  
  private List<HopInvalidatedBlock> findAllInvalidatedBlocks() throws PersistanceException{
    return (List<HopInvalidatedBlock>) EntityManager.findList(HopInvalidatedBlock.Finder.All);
  }
  
}
