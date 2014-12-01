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

import se.sics.hop.metadata.hdfs.entity.FinderType;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
final class HopsSqlBatchedBlocksRelatedLock extends HopsLockWithType {

  HopsSqlBatchedBlocksRelatedLock(FinderType finder, Type type) {
    super(type, finder);
  }

  @Override
  void acquire(TransactionLocks locks) throws Exception {
    HopsLock inodeLock = locks.getLock(Type.INode);
    if (inodeLock instanceof HopsBatchedINodeLock) {
      int[] inodeIds = ((HopsBatchedINodeLock) inodeLock).getINodeIds();
      acquireLockList(DEFAULT_LOCK_TYPE, finder, inodeIds);
    } else {
      throw new TransactionLocks.LockNotAddedException("HopsBatchedINodeLock wasn't added");
    }
  }
}
