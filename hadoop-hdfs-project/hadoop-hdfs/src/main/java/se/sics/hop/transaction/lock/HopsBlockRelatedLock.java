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
import se.sics.hop.metadata.hdfs.entity.FinderType;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
final class HopsBlockRelatedLock extends HopsLockWithType {

  private final boolean isList;

  HopsBlockRelatedLock(boolean isList, FinderType finderType, Type type) {
    super(type, finderType);
    this.isList = isList;
  }
  
   HopsBlockRelatedLock(FinderType finderType, Type type) {
    super(type, finderType);
    this.isList = true;
  }

  @Override
  void acquire(TransactionLocks locks) throws Exception {
    HopsBlockLock blockLock = (HopsBlockLock) locks.getLock(Type.Block);

    for (BlockInfo blk : blockLock.getBlocks()) {
      if (isList) {
        acquireLockList(DEFAULT_LOCK_TYPE, finder, blk.getBlockId(), blk.getInodeId());
      } else {
        acquireLock(DEFAULT_LOCK_TYPE, finder, blk.getBlockId(), blk.getInodeId());
      }
    }
  }

  /*final static class HopsReplicaLock extends HopsBlockRelatedLock {

    HopsReplicaLock() {
      super(true, HopIndexedReplica.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.Replica;
    }
  }

  final static class HopsCorruptReplicaLock extends HopsBlockRelatedLock {

    HopsCorruptReplicaLock() {
      super(true, HopCorruptReplica.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.CorruptReplica;
    }
  }

  final static class HopsExcessReplicaLock extends HopsBlockRelatedLock {

    HopsExcessReplicaLock() {
      super(true, HopExcessReplica.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.ExcessReplica;
    }
  }

  final static class HopsReplicatUnderConstructionLock extends HopsBlockRelatedLock {

    HopsReplicatUnderConstructionLock() {
      super(true, ReplicaUnderConstruction.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.ReplicaUnderConstruction;
    }
  }

  final static class HopsInvalidatedBlockLock extends HopsBlockRelatedLock {

    HopsInvalidatedBlockLock() {
      super(true, HopInvalidatedBlock.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.InvalidatedBlock;
    }
  }

  final static class HopsUnderReplicatedBlockLock extends HopsBlockRelatedLock {

    HopsUnderReplicatedBlockLock() {
      super(false, HopUnderReplicatedBlock.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.UnderReplicatedBlock;
    }
  }

  final static class HopsPendingBlockLock extends HopsBlockRelatedLock {

    HopsPendingBlockLock() {
      super(false, PendingBlockInfo.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.PendingBlock;
    }
  }*/
}
