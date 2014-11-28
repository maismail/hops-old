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

import java.io.IOException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
public abstract class HopsBlockRelatedLock extends HopsLock {

  private final boolean isList;
  private final FinderType finderType;

  public HopsBlockRelatedLock(boolean isList, FinderType finderType) {
    this.isList = isList;
    this.finderType = finderType;
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    HopsBlockLock blockLock = (HopsBlockLock) locks.getLock(Type.Block);

    for (BlockInfo blk : blockLock.getBlocks()) {
      if (isList) {
        acquireLockList(DEFAULT_LOCK_TYPE, finderType, blk.getBlockId(), blk.getInodeId());
      } else {
        acquireLock(DEFAULT_LOCK_TYPE, finderType, blk.getBlockId(), blk.getInodeId());
      }
    }
  }

  public final static class HopsReplicaLock extends HopsBlockRelatedLock {

    public HopsReplicaLock() {
      super(true, HopIndexedReplica.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.Replica;
    }
  }

  public final static class HopsCorruptReplicaLock extends HopsBlockRelatedLock {

    public HopsCorruptReplicaLock() {
      super(true, HopCorruptReplica.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.CorruptReplica;
    }
  }

  public final static class HopsExcessReplicaLock extends HopsBlockRelatedLock {

    public HopsExcessReplicaLock() {
      super(true, HopExcessReplica.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.ExcessReplica;
    }
  }

  public final static class HopsReplicatUnderConstructionLock extends HopsBlockRelatedLock {

    public HopsReplicatUnderConstructionLock() {
      super(true, ReplicaUnderConstruction.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.ReplicaUnderConstruction;
    }
  }

  public final static class HopsInvalidatedBlockLock extends HopsBlockRelatedLock {

    public HopsInvalidatedBlockLock() {
      super(true, HopInvalidatedBlock.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.InvalidatedBlock;
    }
  }

  public final static class HopsUnderReplicatedBlockLock extends HopsBlockRelatedLock {

    public HopsUnderReplicatedBlockLock() {
      super(false, HopUnderReplicatedBlock.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.UnderReplicatedBlock;
    }
  }

  public final static class HopsPendingBlockLock extends HopsBlockRelatedLock {

    public HopsPendingBlockLock() {
      super(false, PendingBlockInfo.Finder.ByBlockId);
    }

    @Override
    final Type getType() {
      return Type.PendingBlock;
    }
  }
}
