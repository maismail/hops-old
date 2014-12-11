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

import se.sics.hop.memcache.Pair;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;

import java.io.IOException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
abstract class HopsBatchedBlocksRelatedLock extends HopsLock {

  private final FinderType finder;
  private final int storageId;

  public HopsBatchedBlocksRelatedLock(FinderType finder, int storageId) {
    this.finder = finder;
    this.storageId = storageId;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    HopsLock blockLock = locks.getLock(Type.Block);
    if (blockLock instanceof HopsBatchedBlockLock) {
      Pair<int[], long[]> inodeBlockIds = ((HopsBatchedBlockLock) blockLock).getINodeBlockIds();
      acquireLockList(DEFAULT_LOCK_TYPE, finder, inodeBlockIds.getR(), inodeBlockIds.getL(), storageId);
    } else {
      throw new TransactionLocks.LockNotAddedException("HopsBatchedBlockLock wasn't added");
    }
  }

  final static class HopsBatchedReplicasLock extends HopsBatchedBlocksRelatedLock {

    public HopsBatchedReplicasLock(int storageId) {
      super(HopIndexedReplica.Finder.ByPKS, storageId);
    }

    @Override
    protected Type getType() {
      return Type.Replica;
    }
  }

  final static class HopsBatchedInvalidatedBlocksLock extends HopsBatchedBlocksRelatedLock {

    public HopsBatchedInvalidatedBlocksLock(int storageId) {
      super(HopInvalidatedBlock.Finder.ByPKS, storageId);
    }

    @Override
    protected Type getType() {
      return Type.InvalidatedBlock;
    }
  }
}
