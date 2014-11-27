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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HopsBlockLock extends HopsLock {

  protected final List<BlockInfo> blocks;

  public HopsBlockLock() {
    this.blocks = new ArrayList<BlockInfo>();
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    HopsINodeLock inodeLock = (HopsINodeLock) locks.getLock(Type.INode);
    for (INode inode :  inodeLock.getResolvedINodesMap().getAll()) {
      if (inode instanceof INodeFile) {
        blocks.addAll(acquireLockList(DEFAULT_LOCK_TYPE, BlockInfo.Finder.ByInodeId, inode.getId()));
      }
    }
  }

  @Override
  Type getType() {
    return Type.Block;
  }
  
  Collection<BlockInfo> getBlocks(){
    return blocks;
  }
}
