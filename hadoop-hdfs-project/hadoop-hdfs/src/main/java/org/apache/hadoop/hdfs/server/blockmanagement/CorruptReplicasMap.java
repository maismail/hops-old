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

import se.sics.hop.metadata.entity.hop.HopCorruptReplica;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import java.util.*;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.metadata.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.StorageFactory;

/**
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are
 * corrupt. While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have 
 * expected number of good replicas.
 * Mapping: Block -> TreeSet<DatanodeDescriptor> 
 */

@InterfaceAudience.Private
public class CorruptReplicasMap{
  
  private final DatanodeManager datanodeMgr;
  
  public CorruptReplicasMap(DatanodeManager datanodeMgr){
    this.datanodeMgr = datanodeMgr;
  }
  /**
   * Mark the block belonging to datanode as corrupt.
   *
   * @param blk Block to be added to CorruptReplicasMap
   * @param dn DatanodeDescriptor which holds the corrupt replica
   * @param reason a textual reason (for logging purposes)
   */
  public void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn,
      String reason) throws PersistanceException {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    
    String reasonText;
    if (reason != null) {
      reasonText = " because " + reason;
    } else {
      reasonText = "";
    }
    
    if (!nodes.contains(dn)) {
      addCorruptReplicaToDB(new HopCorruptReplica(blk.getBlockId(), dn.getStorageID()));
      NameNode.blockStateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   blk.getBlockName() +
                                   " added as corrupt on " + dn +
                                   " by " + Server.getRemoteIp() +
                                   reasonText);
    } else {
      NameNode.blockStateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   "duplicate requested for " + 
                                   blk.getBlockName() + " to add as corrupt " +
                                   "on " + dn +
                                   " by " + Server.getRemoteIp() +
                                   reasonText);
    }
  }

  /**
   * Remove Block from CorruptBlocksMap
   *
   * @param blk Block to be removed
   */
  void removeFromCorruptReplicasMap(Block blk) throws PersistanceException {
    Collection<HopCorruptReplica> corruptReplicas = getCorruptReplicas(blk);
    if (corruptReplicas != null) {
      for (HopCorruptReplica cr : corruptReplicas) {
        removeCorruptReplicaFromDB(cr);
      }
    }
  }

  /**
   * Remove the block at the given datanode from CorruptBlockMap
   * @param blk block to be removed
   * @param datanode datanode where the block is located
   * @return true if the removal is successful; 
             false if the replica is not in the map
   */ 
  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode) throws PersistanceException {  
    Collection<DatanodeDescriptor> datanodes = getNodes(blk);
    if (datanodes == null) {
      return false;
    }

    if (datanodes.contains(datanode)) {
      removeCorruptReplicaFromDB(new HopCorruptReplica(blk.getBlockId(), datanode.getStorageID()));
      return true;
    } else {
      return false;
    }
  }
    

  /**
   * Get Nodes which have corrupt replicas of Block
   * 
   * @param blk Block for which nodes are requested
   * @return collection of nodes. Null if does not exists
   */
  Collection<DatanodeDescriptor> getNodes(Block blk) throws PersistanceException {
   
    //HOPS datanodeMgr is null in some tests
    if (datanodeMgr == null) return new ArrayList<DatanodeDescriptor>();  
    
    Collection<HopCorruptReplica> corruptReplicas = getCorruptReplicas(blk);
    Collection<DatanodeDescriptor> dnds = new TreeSet<DatanodeDescriptor>();
    if(corruptReplicas != null){
      for(HopCorruptReplica cr : corruptReplicas){
        DatanodeDescriptor dn = datanodeMgr.getDatanode(cr.getStorageId());
        if(dn!=null){
            dnds.add(dn);
        }
      }
    }
    return dnds;
  }

  /**
   * Check if replica belonging to Datanode is corrupt
   *
   * @param blk Block to check
   * @param node DatanodeDescriptor which holds the replica
   * @return true if replica is corrupt, false if does not exists in this map
   */
  boolean isReplicaCorrupt(Block blk, DatanodeDescriptor node) throws PersistanceException {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return ((nodes != null) && (nodes.contains(node)));
  }

  public int numCorruptReplicas(Block blk) throws PersistanceException {
    Collection<DatanodeDescriptor> nodes = getNodes(blk);
    return (nodes == null) ? 0 : nodes.size();
  }
  
  public int size() throws IOException{
    return (Integer) new LightWeightRequestHandler(HDFSOperationType.COUNT_CORRUPT_REPLICAS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        CorruptReplicaDataAccess da = (CorruptReplicaDataAccess) StorageFactory.getDataAccess(CorruptReplicaDataAccess.class);
        return da.countAllUniqueBlk();
      }
    }.handle(null);
  }

  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks 
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId 
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks Number of block ids to return.
   *  0 <= numExpectedBlocks <= 100
   * @param startingBlockId Block id from which to start. If null, start at
   *  beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   *
   */
  //FIXME default behaviour was that the order of blocks in the map was according 
  //to when they were added to the list. To do that we will need time stamp column
  //the corrupt replicas table. right now the blocks are sorted by the ids. 
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) throws IOException {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    SortedSet<Long> sortedIds = new TreeSet<Long>();
    
    Collection<HopCorruptReplica> corruptReplicas = getAllCorruptReplicas();
    if (corruptReplicas != null) {
      for (HopCorruptReplica replica : corruptReplicas) {
        sortedIds.add(replica.getBlockId());
      }
    }
    
    Iterator<Long> blockIt = sortedIds.iterator();
    
    // if the starting block id was specified, iterate over keys until
    // we find the matching block. If we find a matching block, break
    // to leave the iterator on the next block after the specified block. 
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        Long bid = blockIt.next();
        if (bid == startingBlockId) {
          isBlockFound = true;
          break; 
        }
      }
      
      if (!isBlockFound) {
        return null;
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<Long>();

    // append up to numExpectedBlocks blockIds to our list
    for(int i=0; i<numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next());
    }
    
    long[] ret = new long[corruptReplicaBlockIds.size()];
    for(int i=0; i<ret.length; i++) {
      ret[i] = corruptReplicaBlockIds.get(i);
    }
    
    return ret;
  }  
  
  private Collection<HopCorruptReplica> getCorruptReplicas(Block blk) throws PersistanceException {
    return EntityManager.findList(HopCorruptReplica.Finder.ByBlockId, blk.getBlockId());
  }

  private Collection<HopCorruptReplica> getAllCorruptReplicas() throws IOException {
    return (Collection<HopCorruptReplica>) new LightWeightRequestHandler(HDFSOperationType.GET_ALL_CORRUPT_REPLICAS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        CorruptReplicaDataAccess crDa = (CorruptReplicaDataAccess) StorageFactory.getDataAccess(CorruptReplicaDataAccess.class);
        return crDa.findAll();
      }
    }.handle(null);
  }
  
  
  private void addCorruptReplicaToDB(HopCorruptReplica cr) throws PersistanceException {
    EntityManager.add(cr);
  }

  private void removeCorruptReplicaFromDB(HopCorruptReplica cr) throws PersistanceException {
    EntityManager.remove(cr);
  }
}
