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
import static org.apache.hadoop.util.Time.now;

import java.io.PrintWriter;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.metadata.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.transaction.lock.TransactionLocks;

/***************************************************
 * PendingReplicationBlocks does the bookkeeping of all
 * blocks that are getting replicated.
 *
 * It does the following:
 * 1)  record blocks that are getting replicated at this instant.
 * 2)  a coarse grain timer to track age of replication request
 * 3)  a thread that periodically identifies replication-requests
 *     that never made it.
 *
 ***************************************************/
class PendingReplicationBlocks {
  private static final Log LOG = BlockManager.LOG;
  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private static long timeout = 5 * 60 * 1000;

  PendingReplicationBlocks(long timeoutPeriod) {
    if ( timeoutPeriod > 0 ) {
      this.timeout = timeoutPeriod;
    }
  }

  void start() {
    
  }

  /**
   * Add a block to the list of pending Replications
   */
  void increment(Block block, int numReplicas) throws PersistanceException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found == null) {
      addPendingBlockInfo(new PendingBlockInfo(block.getBlockId(), now(), numReplicas));
    } else {
      found.incrementReplicas(numReplicas);
      found.setTimeStamp(now());
      updatePendingBlockInfo(found);
    }
  }

  /**
   * One replication request for this block has finished.
   * Decrement the number of pending replication requests
   * for this block.
   */
  void decrement(Block block) throws PersistanceException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found != null && !isTimedout(found)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing pending replication for " + block);
      }
      found.decrementReplicas();
      if (found.getNumReplicas() <= 0) {
        removePendingBlockInfo(found);
      } else {
        updatePendingBlockInfo(found);
      }
    }
  }

  /**
   * Remove the record about the given block from pendingReplications.
   * @param block The given block whose pending replication requests need to be
   *              removed
   */
  void remove(Block block) throws PersistanceException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found != null) {
      removePendingBlockInfo(found);
    }
  }

  public void clear() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DEL_ALL_PENDING_REPL_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        PendingBlockDataAccess da = (PendingBlockDataAccess) StorageFactory.getDataAccess(PendingBlockDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }

  /**
   * The total number of blocks that are undergoing replication
   */
  int size() throws IOException {
    return (Integer) new LightWeightRequestHandler(HDFSOperationType.COUNT_ALL_VALID_PENDING_REPL_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        PendingBlockDataAccess da = (PendingBlockDataAccess) StorageFactory.getDataAccess(PendingBlockDataAccess.class);
        return da.countValidPendingBlocks(getTimeLimit());
      }
    }.handle();
  } 

  /**
   * How many copies of this block is pending replication?
   */
  int getNumReplicas(Block block) throws PersistanceException {
    PendingBlockInfo found = getPendingBlock(block);
    if (found != null && !isTimedout(found)) {
      return found.getNumReplicas();
    }
    return 0;
  }

  /**
   * Returns a list of blocks that have timed out their 
   * replication requests. Returns null if no blocks have
   * timed out.
   */
  Block[] getTimedOutBlocks() throws IOException {
    List<PendingBlockInfo> timedOutItems = (List<PendingBlockInfo>) new LightWeightRequestHandler(HDFSOperationType.GET_TIMED_OUT_PENDING_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        long timeLimit = getTimeLimit();
        PendingBlockDataAccess da = (PendingBlockDataAccess) StorageFactory.getDataAccess(PendingBlockDataAccess.class);
        List<PendingBlockInfo> timedoutPendings = (List<PendingBlockInfo>) da.findByTimeLimitLessThan(timeLimit);
        if (timedoutPendings == null || timedoutPendings.size() <= 0) {
          return null;
        }
        List<PendingBlockInfo> EMPTY = Collections.unmodifiableList(new ArrayList<PendingBlockInfo>());
        da.prepare(timedoutPendings, EMPTY, EMPTY); // remove
        return timedoutPendings;
      }
    }.handle();
    if (timedOutItems == null) {
      return null;
    }
    List<Block> blockList = new ArrayList<Block>();
    for (int i = 0; i < timedOutItems.size(); i++) {
      Block blk = getBlock(timedOutItems.get(i));
      if(blk != null){
        blockList.add(blk);
      }
    }
    Block[] blockArr = new Block[blockList.size()];
    return blockList.toArray(blockArr);
  }
  /*
   * Shuts down the pending replication monitor thread.
   * Waits for the thread to exit.
   */
  void stop() {
  }

  /**
   * Iterate through all items and print them.
   */
  void metaSave(PrintWriter out) throws PersistanceException {
    List<PendingBlockInfo> pendingBlocks = getAllPendingBlocks();
    if (pendingBlocks != null) {
      out.println("Metasave: Blocks being replicated: "
              + pendingBlocks.size());
      for (PendingBlockInfo pendingBlock : pendingBlocks) {
        if (!isTimedout(pendingBlock)) {
          BlockInfo bInfo = getBlockInfo(pendingBlock);
          out.println(bInfo
                  + " StartTime: " + new Time(pendingBlock.getTimeStamp())
                  + " NumReplicaInProgress: "
                  + pendingBlock.getNumReplicas());
        }
      }
    }
  }
  

  private boolean isTimedout(PendingBlockInfo pendingBlock) {
    if (getTimeLimit() > pendingBlock.getTimeStamp()) {
      return true;
    }
    return false;
  }

  private static long getTimeLimit() {
    return now() - timeout;
  }

  private PendingBlockInfo getPendingBlock(Block block) throws PersistanceException {
    return EntityManager.find(PendingBlockInfo.Finder.ByPKey, block.getBlockId());
  }

  private List<PendingBlockInfo> getAllPendingBlocks() throws PersistanceException {
    return (List<PendingBlockInfo>) EntityManager.findList(PendingBlockInfo.Finder.All);
  }

  private BlockInfo getBlockInfo(PendingBlockInfo pendingBlock) throws PersistanceException {
    return EntityManager.find(BlockInfo.Finder.ById, pendingBlock.getBlockId());
  }

  private void addPendingBlockInfo(PendingBlockInfo pbi) throws PersistanceException {
    EntityManager.add(pbi);
  }

  private void updatePendingBlockInfo(PendingBlockInfo pbi) throws PersistanceException {
    EntityManager.update(pbi);
  }

  private void removePendingBlockInfo(PendingBlockInfo pbi) throws PersistanceException {
    EntityManager.remove(pbi);
  }
  
  private Block getBlock(final PendingBlockInfo pbi) throws IOException {
    return (Block) new HDFSTransactionalRequestHandler(HDFSOperationType.GET_BLOCK) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException { 
        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
        tla.getLocks().addBlock(LockType.READ_COMMITTED, pbi.getBlockId());
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Block block = EntityManager.find(BlockInfo.Finder.ById, pbi.getBlockId());
        if (block == null) {
          //this function is called from getTimedOutBlocks
          //which has already deleted the timeout rows from the table
            
          // the block is not found then instead of returning null we have create 
          // the block and return it. 
          // Note: we dont have any information other than block id. infuture this
          // can potentially cause problems
          block = new Block(pbi.getBlockId());
        }
        return block;
      }
    }.handle();
  }
}
