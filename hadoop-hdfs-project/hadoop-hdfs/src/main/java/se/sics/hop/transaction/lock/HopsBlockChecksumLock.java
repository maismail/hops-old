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
package se.sics.hop.transaction.lock;

import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.BlockChecksum;

import java.io.IOException;

public class HopsBlockChecksumLock extends HopsLock {
  private final String target;
  private final int blockIndex;

  public HopsBlockChecksumLock(String target, int blockIndex) {
    this.target = target;
    this.blockIndex = blockIndex;
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    HopsINodeLock iNodeLock = (HopsINodeLock) locks.getLock(Type.INode);
    INode iNode = iNodeLock.getTargetINodes(target);
    BlockChecksumDataAccess.KeyTuple key = new BlockChecksumDataAccess
        .KeyTuple(iNode.getId(), blockIndex);
    acquireLock(DEFAULT_LOCK_TYPE, BlockChecksum.Finder.ByKeyTuple, key);
  }

  @Override
  final Type getType() {
    return Type.BlockChecksum;
  }
}
