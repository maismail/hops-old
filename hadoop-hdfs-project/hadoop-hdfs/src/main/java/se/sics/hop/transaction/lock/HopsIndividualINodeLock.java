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
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.lock.INodeUtil;
import se.sics.hop.transaction.lock.TransactionLockTypes.*;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
public class HopsIndividualINodeLock extends HopsBaseINodeLock {

  private final INodeLockType lockType;
  private final INodeIdentifier inodeIdentifier;
  private final boolean readUpPathInodes;

  public HopsIndividualINodeLock(INodeLockType lockType, INodeIdentifier inodeIdentifier, boolean readUpPathInodes) {
    this.lockType = lockType;
    this.inodeIdentifier = inodeIdentifier;
    this.readUpPathInodes = readUpPathInodes;
    if (lockType.equals(INodeLockType.WRITE_ON_PARENT)) {
      throw new UnsupportedOperationException();
    }
  }

  public HopsIndividualINodeLock(INodeLockType lockType, INodeIdentifier inodeIdentifier) {
    this(lockType, inodeIdentifier, false);
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    setPartitioningKey(inodeIdentifier.getInodeId());

    INode inode = null;
    if (inodeIdentifier.getName() != null && inodeIdentifier.getPid() != null) {
      inode = find(lockType, inodeIdentifier.getName(), inodeIdentifier.getPid());
    } else if (inodeIdentifier.getInodeId() != null) {
      inode = find(lockType, inodeIdentifier.getInodeId());
    } else {
      throw new StorageException("INodeIdentifier objec is not properly initialized ");
    }

    if (inode == null) {
      //there's no inode for this specific name,parentid or inodeId which means this file is deleted
      //so fallback to the scan to update the inodecontext cache
      throw new StorageException("Abort the transaction because INode doesn't exists for " + inodeIdentifier);
    }

    if (readUpPathInodes) {
      List<INode> pathInodes = readUpInodes(inode);
      getResolvedINodesMap().putPathINodes(INodeUtil.constructPath(pathInodes),
          pathInodes);
    } else {
      getResolvedINodesMap().putIndividualINode(inode);
    }
  }

  private List<INode> readUpInodes(INode leaf) throws PersistanceException {
    LinkedList<INode> pathInodes = new LinkedList<INode>();
    pathInodes.add(leaf);
    INode curr = leaf;
    while (curr.getParentId() != INodeDirectory.ROOT_PARENT_ID) {
      curr = find(INodeLockType.READ_COMMITTED, curr.getParentId());
      pathInodes.addFirst(curr);
    }
    return pathInodes;
  }
}
