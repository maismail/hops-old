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
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.INodeIdentifier;
import static se.sics.hop.transaction.lock.HopsLock.DEFAULT_LOCK_TYPE;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HopsIndividualBlockLock extends HopsBlockLock {

  private final long blockId;
  private final int inodeId;

  public HopsIndividualBlockLock(long blockId, INodeIdentifier inode) {
    this.blockId = blockId;
    this.inodeId = inode == null ? INode.NON_EXISTING_ID : inode.getInodeId();
  }

  @Override
  void acquire(TransactionLocks locks) throws Exception {
    BlockInfo result = acquireLock(DEFAULT_LOCK_TYPE, BlockInfo.Finder.ById, blockId, inodeId);
    if (result != null) {
      blocks.add(result);
    }else{
      //TODO fix this add a method to bring null in the others caches 
       BlockInfo dummy = new BlockInfo();
       dummy.setINodeIdNoPersistance(inodeId);
       dummy.setBlockIdNoPersistance(blockId);
       blocks.add(result);
    }
  }
}
