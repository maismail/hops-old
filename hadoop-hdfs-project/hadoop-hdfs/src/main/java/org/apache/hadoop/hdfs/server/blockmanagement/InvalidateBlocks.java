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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import se.sics.hop.metadata.persistence.EntityManager;
import se.sics.hop.metadata.persistence.LightWeightRequestHandler;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.metadata.persistence.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InvalidateBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

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
    return (Integer) new LightWeightRequestHandler(OperationType.GET_NUM_INVALIDATED_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        return da.countAll();
      }
    }.handle(null);
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
  boolean contains(final String storageID, final Block block) throws PersistanceException {
    InvalidatedBlock blkFound = findBlock(block.getBlockId(), storageID);
    if (blkFound == null) {
      return false;
    }
    return blkFound.getGenerationStamp() == block.getGenerationStamp();
  }

  /**
   * Add a block to the block collection
   * which will be invalidated on the specified datanode.
   */
  void add(final Block block, final DatanodeInfo datanode,
      final boolean log) throws PersistanceException {
    InvalidatedBlock invBlk = new InvalidatedBlock(datanode.getStorageID(), block.getBlockId(), block.getGenerationStamp(), block.getNumBytes());
    if (add(invBlk)) {
      if (log) {
        NameNode.blockStateChangeLog.info("BLOCK* " + getClass().getSimpleName()
                + ": add " + block + " to " + datanode);
      }
    }
  }

  /** Remove a storage from the invalidatesSet */
  void remove(final String storageID) throws IOException {
    List<InvalidatedBlock> invBlocks = findInvBlocksbyStorageId(storageID);
    if(invBlocks != null){
      for(InvalidatedBlock invBlk : invBlocks){
        if(invBlk != null){
          removeInvBlockTx(invBlk);
        }
      }
    }
  }

  /** Remove the block from the specified storage. */
  void remove(final String storageID, final Block block) throws PersistanceException {
    removeInvalidatedBlockFromDB(new InvalidatedBlock(storageID, block.getBlockId(), block.getGenerationStamp(), block.getNumBytes()));
  }

  /** Print the contents to out. */
  void dump(final PrintWriter out) throws PersistanceException {
    List<InvalidatedBlock> invBlocks = findAllInvalidatedBlocks();
    HashSet<String> storageIds = new HashSet<String>();
    for (InvalidatedBlock ib : invBlocks) {
      storageIds.add(ib.getStorageId());
    }
    final int size = storageIds.size();
    out.println("Metasave: Blocks " + invBlocks.size()
            + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }

    for (String sId : storageIds) {
      HashSet<InvalidatedBlock> invSet = new HashSet<InvalidatedBlock>();
      for (InvalidatedBlock ib : invBlocks) {
        if (ib.getStorageId().equals(sId)) {
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
    LightWeightRequestHandler getAllInvBlocksHandler = new LightWeightRequestHandler(OperationType.GET_ALL_INV_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        return da.findAllInvalidatedBlocks();
      }
    };
    List<InvalidatedBlock> invBlocks = (List<InvalidatedBlock>) getAllInvBlocksHandler.handle(null);
    HashSet<String> storageIds = new HashSet<String>();
    if (invBlocks != null) {
      for (InvalidatedBlock ib : invBlocks) {
        storageIds.add(ib.getStorageId());
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
    final List<Block> toInvalidate = invalidateWork(storageId, dn);
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
      final String storageId, final DatanodeDescriptor dn) throws IOException {
    final List<InvalidatedBlock> invBlocks = findInvBlocksbyStorageId(storageId);
    if (invBlocks == null || invBlocks.isEmpty()) {
      return null;
    }
    // # blocks that can be sent in one message is limited
    final int limit = datanodeManager.blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<Block>(limit);
    final Iterator<InvalidatedBlock> it = invBlocks.iterator();
    for (int count = 0; count < limit && it.hasNext(); count++) {
      InvalidatedBlock invBlock = it.next();
      toInvalidate.add(new Block(invBlock.getBlockId(),
              invBlock.getNumBytes(), invBlock.getGenerationStamp()));
      removeInvBlockTx(invBlock);
    }
    dn.addBlocksToBeInvalidated(toInvalidate);
    return toInvalidate;
  }
  
  void clear() throws IOException {
    new LightWeightRequestHandler(OperationType.DEL_ALL_INV_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle(null);
  }
  
  
  
  private boolean add(InvalidatedBlock invBlk) throws PersistanceException {
    InvalidatedBlock found = findBlock(invBlk.getBlockId(), invBlk.getStorageId());
    if (found == null) {
      addInvalidatedBlockToDB(invBlk);
      return true;
    }
    return false;
  }
  
  private List<InvalidatedBlock> findInvBlocksbyStorageId(final String sid) throws IOException {
    return (List<InvalidatedBlock>) new LightWeightRequestHandler(OperationType.GET_INV_BLKS_BY_STORAGEID) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
        return da.findInvalidatedBlockByStorageId(sid);
      }
    }.handle(null);
  }

  private void removeInvBlockTx(final InvalidatedBlock ib) throws IOException {    
     new LightWeightRequestHandler(OperationType.RM_INV_BLK) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
       InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
       da.remove(ib);
       return null;
      }
    }.handle(null);
  }
  
  private InvalidatedBlock findBlock(long blkId, String storageID) throws PersistanceException {
    return (InvalidatedBlock) EntityManager.find(InvalidatedBlock.Finder.ByPrimaryKey, blkId, storageID);
  }
  
  private void addInvalidatedBlockToDB(InvalidatedBlock invBlk) throws PersistanceException {
    EntityManager.add(invBlk);
  }
  
  private void removeInvalidatedBlockFromDB(InvalidatedBlock invBlk) throws PersistanceException {
    EntityManager.remove(invBlk);
  }
  
  private List<InvalidatedBlock> findAllInvalidatedBlocks() throws PersistanceException{
    return (List<InvalidatedBlock>) EntityManager.findList(InvalidatedBlock.Finder.All);
  }
  
}
