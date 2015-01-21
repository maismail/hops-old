/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.transaction.lock;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;

import java.io.IOException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
final class HopsBlockRelatedLock extends HopsLockWithType {

  HopsBlockRelatedLock(Type type) {
    super(type);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    // FIXME handle null block
    HopsLock lock = locks.getLock(Type.Block);
    if (lock instanceof HopsBaseIndividualBlockLock) {
      HopsBaseIndividualBlockLock individualBlockLock = (HopsBaseIndividualBlockLock) lock;
      //get by blocksId
      for (BlockInfo blk : individualBlockLock.getBlocks()) {
        if (isList()) {
          acquireLockList(DEFAULT_LOCK_TYPE, getFinderType(true), blk.getBlockId(), blk.getInodeId());
        } else {
          acquireLock(DEFAULT_LOCK_TYPE, getFinderType(true), blk.getBlockId(), blk.getInodeId());
        }
      }
      if (lock instanceof HopsBlockLock) {
        //get by inodeId
        HopsBlockLock blockLock = (HopsBlockLock) lock;
        for (INodeFile file : blockLock.getFiles()) {
          acquireLockList(DEFAULT_LOCK_TYPE, getFinderType(false), file.getId());
        }
      }
    } else {
      throw new TransactionLocks.LockNotAddedException("Block Lock wasn't added");
    }
  }

  private FinderType getFinderType(boolean byBlockID) {
    switch (getType()) {
      case Replica:
        return byBlockID ? HopIndexedReplica.Finder.ByBlockIdAndINodeId : HopIndexedReplica.Finder.ByINodeId;
      case CorruptReplica:
        return byBlockID ? HopCorruptReplica.Finder.ByBlockIdAndINodeId : HopCorruptReplica.Finder.ByINodeId;
      case ExcessReplica:
        return byBlockID ? HopExcessReplica.Finder.ByBlockIdAndINodeId : HopExcessReplica.Finder.ByINodeId;
      case ReplicaUnderConstruction:
        return byBlockID ? ReplicaUnderConstruction.Finder.ByBlockIdAndINodeId : ReplicaUnderConstruction.Finder.ByINodeId;
      case InvalidatedBlock:
        return byBlockID ? HopInvalidatedBlock.Finder.ByBlockIdAndINodeId : HopInvalidatedBlock.Finder.ByINodeId;
      case UnderReplicatedBlock:
        return byBlockID ? HopUnderReplicatedBlock.Finder.ByBlockIdAndINodeId : HopUnderReplicatedBlock.Finder.ByINodeId;
      case PendingBlock:
        return byBlockID ? PendingBlockInfo.Finder.ByBlockIdAndINodeId : PendingBlockInfo.Finder.ByINodeId;
    }
    return null;
  }

  private boolean isList() {
    switch (getType()) {
      case UnderReplicatedBlock:
      case PendingBlock:
        return false;
      default:
        return true;
    }
  }
  
}
