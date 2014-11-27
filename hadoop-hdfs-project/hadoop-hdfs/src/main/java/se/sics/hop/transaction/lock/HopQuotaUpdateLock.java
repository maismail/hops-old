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
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.hdfs.entity.hop.QuotaUpdate;
import se.sics.hop.transaction.EntityManager;

import java.io.IOException;
import java.util.List;

public class HopQuotaUpdateLock extends HopsLock {
  private final String[] targets;
  private final boolean includeChildren;

  public HopQuotaUpdateLock(boolean includeChildren, String... targets) {
    this.includeChildren = includeChildren;
    this.targets = targets;
  }

  public HopQuotaUpdateLock(String... paths) {
    this(false, paths);
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    HopsINodeLock inodeLock = (HopsINodeLock) locks.getLock(Type.INode);
    for (String target : targets) {
      acquireQuotaUpdate(inodeLock.getTargetINodes(target));
      if (includeChildren) {
        acquireQuotaUpdate(inodeLock.getChildINodes(target));
      }
    }
  }

  private void acquireQuotaUpdate(List<INode> iNodes) throws PersistanceException {
    for (INode iNode : iNodes) {
      acquireQuotaUpdate(iNode);
    }
  }

  private void acquireQuotaUpdate(INode node) throws PersistanceException {
    EntityManager.findList(QuotaUpdate.Finder.ByInodeId, node.getId());
  }

  @Override
  Type getType() {
    return Type.QuotaUpdate;
  }
}