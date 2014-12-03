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

import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;
import static se.sics.hop.transaction.lock.HopsLock.Type.CorruptReplica;
import static se.sics.hop.transaction.lock.HopsLock.Type.ExcessReplica;
import static se.sics.hop.transaction.lock.HopsLock.Type.InvalidatedBlock;
import static se.sics.hop.transaction.lock.HopsLock.Type.PendingBlock;
import static se.sics.hop.transaction.lock.HopsLock.Type.Replica;
import static se.sics.hop.transaction.lock.HopsLock.Type.UnderReplicatedBlock;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
final class HopsSqlBatchedBlocksRelatedLock extends HopsLockWithType {

  HopsSqlBatchedBlocksRelatedLock(Type type) {
    super(type);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws Exception {
    HopsLock inodeLock = locks.getLock(Type.INode);
    if (inodeLock instanceof HopsBatchedINodeLock) {
      int[] inodeIds = ((HopsBatchedINodeLock) inodeLock).getINodeIds();
      acquireLockList(DEFAULT_LOCK_TYPE, getFinderType(), inodeIds);
    } else {
      throw new TransactionLocks.LockNotAddedException("Batched Inode Lock wasn't added");
    }
  }

  private FinderType getFinderType() {
    switch (getType()) {
      case Replica:
        return HopIndexedReplica.Finder.ByINodeIds;
      case CorruptReplica:
        return HopCorruptReplica.Finder.ByINodeIds;
      case ExcessReplica:
        return HopExcessReplica.Finder.ByINodeIds;
      case ReplicaUnderConstruction:
        return ReplicaUnderConstruction.Finder.ByINodeIds;
      case InvalidatedBlock:
        return HopInvalidatedBlock.Finder.ByINodeIds;
      case UnderReplicatedBlock:
        return HopUnderReplicatedBlock.Finder.ByINodeIds;
      case PendingBlock:
        return PendingBlockInfo.Finder.ByInodeIds;
    }
    return null;
  }
}
