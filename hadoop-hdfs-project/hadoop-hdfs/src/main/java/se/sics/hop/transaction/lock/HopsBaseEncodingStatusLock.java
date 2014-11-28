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
import se.sics.hop.erasure_coding.EncodingStatus;

import java.io.IOException;

public abstract class HopsBaseEncodingStatusLock extends HopsLock {

  @Override
  final Type getType() {
    return Type.EncodingStatus;
  }

  public static class HopsEncodingStatusLock extends
      HopsBaseEncodingStatusLock {
    private final String[] targets;

    public HopsEncodingStatusLock(String... targets) {
      this.targets = targets;
    }

    @Override
    void acquire(TransactionLocks locks) throws IOException {
      HopsINodeLock iNodeLock = (HopsINodeLock) locks.getLock(Type.INode);
      for (String target : targets) {
        INode iNode = iNodeLock.getTargetINodes(target);
        acquireLock(DEFAULT_LOCK_TYPE, EncodingStatus.Finder.ByInodeId, iNode.getId());
      }
    }
  }

  public static class HopsIndividualEncodingStatusLock extends
      HopsBaseEncodingStatusLock {
    private final int inodeId;

    public HopsIndividualEncodingStatusLock(int inodeId) {
      this.inodeId = inodeId;
    }

    @Override
    void acquire(TransactionLocks locks) throws IOException {
      // TODO STEFFEN - Should only acquire the locks if we know it has a status and also not twice.
      // Maybe add a flag to iNode specifying whether it's encoded or a parity file
      EncodingStatus status = acquireLock(DEFAULT_LOCK_TYPE,
          EncodingStatus.Finder.ByInodeId, inodeId);
      if (status != null) {
        // It's a source file
        return;
      }
      // It's a parity file
      acquireLock(DEFAULT_LOCK_TYPE, EncodingStatus.Finder.ByParityInodeId, inodeId);
    }
  }
}
