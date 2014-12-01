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
import se.sics.hop.memcache.Pair;
import static se.sics.hop.transaction.lock.HopsLock.DEFAULT_LOCK_TYPE;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
final class HopsBatchedBlockLock extends HopsBlockLock {

  private final long[] blockIds;
  private int[] inodeIds;

  public HopsBatchedBlockLock(long[] blockIds, int[] inodeIds) {
    this.blockIds = blockIds;
    this.inodeIds = inodeIds;
  }

  public HopsBatchedBlockLock(long[] blockIds) {
    this(blockIds, null);
  }

  @Override
  void acquire(TransactionLocks locks) throws Exception {
    if(inodeIds == null){
      inodeIds = INodeUtil.resolveINodesFromBlockIds(blockIds);
    }
    blocks.addAll(acquireLockList(DEFAULT_LOCK_TYPE, BlockInfo.Finder.ByIds, blockIds, inodeIds));
  }
  
  Pair<int[], long[]> getINodeBlockIds(){
    return new Pair<int[], long[]>(inodeIds, blockIds);
  }
  
}
