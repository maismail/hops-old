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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import se.sics.hop.common.INodeResolver;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeResolveType;

/**
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
class HopsINodeLock extends HopsBaseINodeLock {
  
  private final INodeLockType lockType;
  private final INodeResolveType resolveType;
  private final boolean resolveLink;
  protected final String[] paths;
  private final Collection<ActiveNamenode> activeNamenodes;
  private final boolean ignoreLocalSubtreeLocks;
  private final long namenodeId;


  HopsINodeLock(INodeLockType lockType, INodeResolveType resolveType,
      boolean resolveLink, boolean ignoreLocalSubtreeLocks, long namenodeId,
      Collection<ActiveNamenode> activeNamenodes, String... paths) {
    super();
    this.lockType = lockType;
    this.resolveType = resolveType;
    this.resolveLink = resolveLink;
    this.activeNamenodes = activeNamenodes;
    this.ignoreLocalSubtreeLocks = ignoreLocalSubtreeLocks;
    this.namenodeId = namenodeId;
    this.paths = paths;
  }

  HopsINodeLock(INodeLockType lockType, INodeResolveType resolveType,
      boolean resolveLink, Collection<ActiveNamenode> activeNamenodes,
      String... paths) {
    this(lockType, resolveType, resolveLink, false, -1, activeNamenodes, paths);
  }

  HopsINodeLock(INodeLockType lockType, INodeResolveType resolveType,
      Collection<ActiveNamenode> activeNamenodes, String... paths) {
    this(lockType, resolveType, true, false, -1, activeNamenodes, paths);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    /*
     * Needs to be sorted in order to avoid deadlocks. Otherwise one transaction
     * could acquire path0 and path1 in the given order while another one does
     * it in the opposite order, more precisely path1, path0, what could cause
     * a dealock situation.
     */
    Arrays.sort(paths);
    setPartitionKey();
    acquireINodeLocks();
    acquireINodeAttributes();
  }
  
  protected void acquireINodeLocks()
      throws UnresolvedPathException, StorageException,
      SubtreeLockedException, TransactionContextException {
    switch (resolveType) {
      case PATH: // Only use memcached for this case.
      case PATH_AND_IMMEDIATE_CHILDREN: // Memcached not applicable for delete of a dir (and its children)
      case PATH_AND_ALL_CHILDREN_RECURSIVELY:
        for (int i = 0; i < paths.length; i++) {
          List<INode> resolvedINodes = acquireINodeLockByPath(paths[i]);
          addPathINodes(paths[i], resolvedINodes);
          if (resolvedINodes.size() > 0) {
            INode lastINode = resolvedINodes.get(resolvedINodes.size() - 1);
            if (resolveType == INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
              List<INode> children = findImmediateChildren(lastINode);
              addChildINodes(paths[i], children);
            } else if (resolveType ==
                INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY) {
              List<INode> children = findChildrenRecursively(lastINode);
              addChildINodes(paths[i], children);
            }
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + lockType.name());
    }
  }

  private List<INode> acquireINodeLockByPath(String path)
      throws UnresolvedPathException, StorageException,
      SubtreeLockedException, TransactionContextException {
    List<INode> resolvedINodes = new ArrayList<INode>();
    byte[][] components = INode.getPathComponents(path);

    INode currentINode;
    if (isRootTarget(components)) {
      resolvedINodes.add(acquireLockOnRoot(lockType));
      return resolvedINodes;
    } else if (isRootParent(components)
        && TransactionLockTypes.impliesParentWriteLock(this.lockType)) {
      currentINode = acquireLockOnRoot(lockType);
    } else {
      currentINode = acquireLockOnRoot(DEFAULT_INODE_LOCK_TYPE);
    }
    resolvedINodes.add(currentINode);

    INodeResolver resolver = new INodeResolver(components, currentINode,
        resolveLink, true);
    while (resolver.hasNext()) {
      INodeLockType currentINodeLock =
          identifyLockType(resolver.getCount() + 1, components);
      setINodeLockType(currentINodeLock);
      currentINode = resolver.next();
      if (currentINode != null) {
        addLockedINodes(currentINode, currentINodeLock);
        checkSubtreeLock(currentINode);
        resolvedINodes.add(currentINode);
      }
    }

    handleLockUpgrade(resolvedINodes, components, path);
    return resolvedINodes;
  }

  private boolean isRootTarget(byte[][] components) {
    return isTarget(0, components);
  }

  private boolean isRootParent(byte[][] components) {
    return isParent(0, components);
  }

  private INodeLockType identifyLockType(int count, byte[][] components)
      throws StorageException {
    INodeLockType lkType;
    if (isTarget(count, components)) {
      lkType = this.lockType;
    } else if (isParent(count, components)
        && TransactionLockTypes.impliesParentWriteLock(this.lockType)) {
      lkType = INodeLockType.WRITE;
    } else {
      lkType = DEFAULT_INODE_LOCK_TYPE;
    }
    return lkType;
  }

  private boolean isTarget(int count, byte[][] components) {
    return count == components.length - 1;
  }

  private boolean isParent(int count, byte[][] components) {
    return count == components.length - 2;
  }

  private void checkSubtreeLock(INode iNode) throws SubtreeLockedException {
    if (SubtreeLockHelper.isSubtreeLocked(iNode.isSubtreeLocked(),
        iNode.getSubtreeLockOwner(), activeNamenodes)) {
      if (!ignoreLocalSubtreeLocks && namenodeId != iNode.getSubtreeLockOwner()) {
        throw new SubtreeLockedException(activeNamenodes);
      }
    }
  }

  private void handleLockUpgrade(List<INode> resolvedINodes, byte[][] components, String path)
      throws StorageException, UnresolvedPathException,
      TransactionContextException {
    // TODO Handle the case that predecessing nodes get deleted before locking
    // lock upgrade if the path was not fully resolved
    if (resolvedINodes.size() != components.length) {
      // path was not fully resolved
      INode inodeToReread = null;
      if (lockType == INodeLockType.WRITE_ON_TARGET_AND_PARENT) {
        if (resolvedINodes.size() <= components.length - 2) {
          inodeToReread = resolvedINodes.get(resolvedINodes.size() - 1);
        }
      } else if (lockType == INodeLockType.WRITE) {
        inodeToReread = resolvedINodes.get(resolvedINodes.size() - 1);
      }

      if (inodeToReread != null) {
        INode inode = find(lockType, inodeToReread.getLocalName(),
            inodeToReread.getParentId());
        if (inode != null) {
        // re-read after taking write lock to make sure that no one has created the same inode.
          addLockedINodes(inode, lockType);
          String existingPath = buildPath(path, resolvedINodes.size());
          List<INode> rest = acquireLockOnRestOfPath(lockType, inode, path,
              existingPath, false);
          resolvedINodes.addAll(rest);
        }
      }
    }
  }

  private List<INode> acquireLockOnRestOfPath(INodeLockType lock,
      INode baseInode, String fullPath, String prefix, boolean resolveLink)
      throws StorageException, UnresolvedPathException,
      TransactionContextException {
    List<INode> resolved = new ArrayList<INode>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    INodeResolver resolver = new INodeResolver(fullComps, baseInode,
        resolveLink, true, prefixComps.length - 1);
    while (resolver.hasNext()) {
      setINodeLockType(lock);
      INode current = resolver.next();
      if (current != null) {
        addLockedINodes(current, lock);
        resolved.add(current);
      }
    }
    return resolved;
  }

  private List<INode> findImmediateChildren(INode lastINode)
      throws StorageException, TransactionContextException {
    List<INode> children = new ArrayList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        setINodeLockType(lockType);
        children.addAll(((INodeDirectory) lastINode).getChildren());
      }
    }
    return children;
  }

  private List<INode> findChildrenRecursively(INode lastINode) throws
      StorageException, TransactionContextException {
    LinkedList<INode> children = new LinkedList<INode>();
    LinkedList<INode> unCheckedDirs = new LinkedList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        unCheckedDirs.add(lastINode);
      }
    }

    // Find all the children in the sub-directories.
    while (!unCheckedDirs.isEmpty()) {
      INode next = unCheckedDirs.poll();
      if (next instanceof INodeDirectory) {
        setINodeLockType(lockType);
        List<INode> clist = ((INodeDirectory) next).getChildren();
        unCheckedDirs.addAll(clist);
        children.addAll(clist);
      }
    }
    LOG.debug("Added " + children.size() + " children.");
    return children;
  }

  private INode acquireLockOnRoot(INodeLockType lock)
      throws StorageException, TransactionContextException {
    LOG.debug("Acquiring " + lock + " on the root node");
    return find(lock, INodeDirectory.ROOT_NAME, INodeDirectory.ROOT_PARENT_ID);
  }

  private String buildPath(String path, int size) {
    StringBuilder builder = new StringBuilder();
    byte[][] components = INode.getPathComponents(path);

    for (int i = 0; i < Math.min(components.length, size); i++) {
      if (i == 0) {
        builder.append("/");
      } else {
        if (i != 1) {
          builder.append("/");
        }
        builder.append(DFSUtil.bytes2String(components[i]));
      }
    }

    return builder.toString();
  }
  
  protected INode find(String name, int parentId)
      throws StorageException, TransactionContextException {
    return find(lockType, name, parentId);
  }
}
