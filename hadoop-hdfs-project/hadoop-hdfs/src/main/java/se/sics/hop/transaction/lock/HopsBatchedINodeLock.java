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

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.transaction.EntityManager;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HopsBatchedINodeLock extends HopsBaseINodeLock {

  private final List<INodeIdentifier> inodeIdentifiers;
  private int[] inodeIds;

  public HopsBatchedINodeLock(List<INodeIdentifier> inodeIdentifiers) {
    this.inodeIdentifiers = inodeIdentifiers;
    inodeIds = new int[inodeIdentifiers.size()];
  }

  @Override
  void acquire(TransactionLocks locks) throws Exception {
    if (inodeIdentifiers != null && !inodeIdentifiers.isEmpty()) {
      String[] names = new String[inodeIdentifiers.size()];
      int[] parentIds = new int[inodeIdentifiers.size()];
      for (int i = 0; i < inodeIdentifiers.size(); i++) {
        INodeIdentifier inodeIdentifier = inodeIdentifiers.get(i);
        names[i] = inodeIdentifier.getName();
        parentIds[i] = inodeIdentifier.getPid();
        inodeIds[i] = inodeIdentifier.getInodeId();
      }

      find(DEFAULT_INODE_LOCK_TYPE, names, parentIds);
    } else {
      throw new StorageException("INodeIdentifier object is not properly initialized ");
    }
  }

  private Collection<INode> find(
          TransactionLockTypes.INodeLockType lock,
          String[] names,
          int[] parentIds)
          throws PersistanceException {
    setINodeLockType(lock);
    Collection<INode> inodes = EntityManager.findList(INode.Finder.ByPKS, names, parentIds);
    for (INode inode : inodes) {
      addLockedINodes(inode, lock);
    }
    return inodes;
  }

  int[] getINodeIds() {
    return inodeIds;
  }
}
