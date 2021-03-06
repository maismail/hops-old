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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {

  private final DatanodeManager datanodeManager;
  private final static List<DatanodeDescriptor> empty_datanode_list = Collections.unmodifiableList(new ArrayList<DatanodeDescriptor>());
  
  BlocksMap(DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  void close() {
    // Empty blocks once GSet#clear is implemented (HDFS-3940)
  }

  BlockCollection getBlockCollection(Block b)
      throws StorageException, TransactionContextException {
    BlockInfo info = getStoredBlock(b);
    return (info != null) ? info.getBlockCollection() : null;
  }

  /**
   * Add block b belonging to the specified block collection to the map.
   */
  BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) throws
      StorageException, TransactionContextException {
    b.setBlockCollection(bc);
    return b;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(Block block)
      throws StorageException, TransactionContextException {
    BlockInfo blockInfo = getStoredBlock(block);
    if (blockInfo == null)
      return;  
    blockInfo.remove();
    blockInfo.removeAllReplicas();
  }
  
  /** Returns the block object it it exists in the map. */
  BlockInfo getStoredBlock(Block b)
      throws StorageException, TransactionContextException {
    // TODO STEFFEN - This is a workaround to prevent NullPointerExceptions for me. Not sure how to actually fix the bug.
    if (b == null) {
      return null;
    }
    if (!(b instanceof BlockInfo)) {
      return EntityManager.find(BlockInfo.Finder.ById, b.getBlockId());
    }
    return (BlockInfo)b;
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns Iterator that iterates through the nodes the block belongs to.
   */
  Iterator<DatanodeDescriptor> nodeIterator(Block b) throws
      StorageException, TransactionContextException {
    BlockInfo blockInfo = getStoredBlock(b);
    return nodeIterator(blockInfo);
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns Iterator that iterates through the nodes the block belongs to.
   */
  Iterator<DatanodeDescriptor> nodeIterator(BlockInfo storedBlock) throws
      StorageException, TransactionContextException {
    if (storedBlock == null) {
      return null;
    }
    DatanodeDescriptor[] desc = storedBlock.getDatanodes(datanodeManager);
    if (desc == null) {
      return empty_datanode_list.iterator();
    } else {
      return Arrays.asList(desc).iterator();
    }
  }

  /** counts number of containing nodes. Better than using iterator. */
  int numNodes(Block b) throws StorageException, TransactionContextException {
    BlockInfo info = getStoredBlock(b);
    return info == null ? 0 : info.numNodes(datanodeManager);
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node) throws
      StorageException, TransactionContextException {
    BlockInfo info = getStoredBlock(b);
    if (info == null)
      return false;

    // remove block from the data-node list and the node from the block info
    boolean removed = node.removeBlock(info);
    return removed;
  }

  int size() throws IOException {
    LightWeightRequestHandler getAllBlocksSizeHander = new LightWeightRequestHandler(HDFSOperationType.GET_ALL_BLOCKS_SIZE) {
      @Override
      public Object performTask() throws IOException {
        BlockInfoDataAccess bida = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
        return bida.countAll();
      }
    };
    return (Integer) getAllBlocksSizeHander.handle();
  }

  int sizeCompleteOnly() throws IOException {
    LightWeightRequestHandler getAllBlocksSizeHander = new LightWeightRequestHandler(HDFSOperationType.GET_COMPLETE_BLOCKS_TOTAL) {
      @Override
      public Object performTask() throws IOException {
        BlockInfoDataAccess bida = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
        return bida.countAllCompleteBlocks();
      }
    };
    return (Integer) getAllBlocksSizeHander.handle();
  }
    
  Iterable<BlockInfo> getBlocks() throws IOException {
    LightWeightRequestHandler getAllBlocksHander = new LightWeightRequestHandler(HDFSOperationType.GET_ALL_BLOCKS) {
      @Override
      public Object performTask() throws IOException {
        BlockInfoDataAccess bida = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
        return bida.findAllBlocks();
      }
    };
    return (List<BlockInfo>) getAllBlocksHander.handle();
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  int getCapacity(){
    return Integer.MAX_VALUE;
  }

  /**
   * Replace a block in the block map by a new block.
   * The new block and the old one have the same key.
   * @param newBlock - block for replacement
   * @return new block
   */
  BlockInfo replaceBlock(BlockInfo newBlock) {
    //HOP: [M] doesn't make sense in our case, beacause the new block will have the same id as the old one
    return newBlock;
  }
  
  List<INodeIdentifier> getAllINodeFiles(final long offset, final long count) throws IOException {
    return (List<INodeIdentifier>) new LightWeightRequestHandler(HDFSOperationType.GET_ALL_INODES) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
        return ida.getAllINodeFiles(offset, count);
      }
    }.handle();
  }
  
  boolean haveFilesWithIdGreaterThan(final long id) throws IOException {
    return (Boolean) new LightWeightRequestHandler(HDFSOperationType.HAVE_FILES_WITH_IDS_GREATER_THAN) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
        return ida.haveFilesWithIdsGreaterThan(id);
      }
    }.handle();
  }
  
   boolean haveFilesWithIdBetween(final long startId, final long endId) throws IOException {
    return (Boolean) new LightWeightRequestHandler(HDFSOperationType.HAVE_FILES_WITH_IDS_BETWEEN) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
        return ida.haveFilesWithIdsBetween(startId, endId);
      }
    }.handle();
  }
   
  long getMinFileId() throws IOException {
    return (Long) new LightWeightRequestHandler(HDFSOperationType.GET_MIN_FILE_ID) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
        return ida.getMinFileId();
      }
    }.handle();
  }

  int countAllFiles() throws IOException {
    return (Integer) new LightWeightRequestHandler(HDFSOperationType.COUNTALL_FILES) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess ida = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
        return ida.countAllFiles();
      }
    }.handle();
  }
}
