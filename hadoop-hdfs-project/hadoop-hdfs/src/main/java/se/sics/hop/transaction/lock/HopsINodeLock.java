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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.metadata.lock.INodeUtil;
import se.sics.hop.metadata.lock.SubtreeLockHelper;
import se.sics.hop.metadata.lock.SubtreeLockedException;
import se.sics.hop.transaction.lock.TransactionLockTypes.*;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeResolveType;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
public class HopsINodeLock extends HopsINodeLockBase {

  private static INodeLockType defaultINodeLockType = INodeLockType.READ_COMMITTED;

  public static void setDefaultLockType(INodeLockType defaultLockType) {
    defaultINodeLockType = defaultLockType;
  }
  
  private final INodeLockType lockType;
  private final INodeResolveType resolveType;
  private final boolean resolveLink;
  private final String[] paths;
  private final List<ActiveNamenode> activeNamenodes;
  private final boolean ignoreLocalSubtreeLocks;
  private final long namenodeId;


  public HopsINodeLock(INodeLockType lockType, INodeResolveType resolveType, boolean resolveLink, boolean ignoreLocalSubtreeLocks, long namenodeId, List<ActiveNamenode> activeNamenodes, String... paths) {
    super();
    this.lockType = lockType;
    this.resolveType = resolveType;
    this.resolveLink = resolveLink;
    this.activeNamenodes = activeNamenodes;
    this.ignoreLocalSubtreeLocks = ignoreLocalSubtreeLocks;
    this.namenodeId = namenodeId;
    this.paths = paths;
  }

  public HopsINodeLock(INodeLockType lockType, INodeResolveType resolveType, boolean resolveLink, String... paths) {
    this(lockType, resolveType, resolveLink, false, -1, null, paths);
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    setPartitionKey();
    acquireInodeLocks();
    acquireINodeAttributes();
  }
  
  private void acquireInodeLocks() throws UnresolvedPathException, PersistanceException, SubtreeLockedException {
    switch (resolveType) {
      case PATH: // Only use memcached for this case.
      case PATH_AND_IMMEDIATE_CHILDREN: // Memcached not applicable for delete of a dir (and its children)
      case PATH_AND_ALL_CHILDREN_RECURESIVELY:
        for (int i = 0; i < paths.length; i++) {
          List<INode> resolvedInodes = acquireInodeLockByPath(paths[i]);
          if (resolvedInodes.size() > 0) {
            INode lastINode = resolvedInodes.get(resolvedInodes.size() - 1);
            if (resolveType == INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
              List<INode> children = findImmediateChildren(lastINode);
              resolvedInodes.addAll(children);
            } else if (resolveType == INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY) {
              Queue<INode> children = findChildrenRecursively(lastINode);
              resolvedInodes.addAll(children);
            }
          }
          addResolvedInodes(resolvedInodes);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + lockType.name());
    }
  }

  private List<INode> acquireInodeLockByPath(String path)
          throws UnresolvedPathException, PersistanceException, SubtreeLockedException {
    List<INode> resolvedInodes = new ArrayList<INode>();

    if (path == null) {
      return resolvedInodes;
    }

    byte[][] components = INode.getPathComponents(path);
    INode[] curNode = new INode[1];

    int[] count = new int[]{0};
    boolean lastComp = (count[0] == components.length - 1);
    if (lastComp) { // if root is the last directory, we should acquire the write lock over the root
      resolvedInodes.add(acquireLockOnRoot(lockType));
      return resolvedInodes;
    } else if ((count[0] == components.length - 2) && lockType == INodeLockType.WRITE_ON_PARENT) { // if Root is the parent
      curNode[0] = acquireLockOnRoot(lockType);
    } else {
      curNode[0] = acquireLockOnRoot(INodeLockType.READ_COMMITTED);
    }
    resolvedInodes.add(curNode[0]);

    while (count[0] < components.length && curNode[0] != null) {

      INodeLockType curInodeLock = null;
      if (((lockType == INodeLockType.WRITE || lockType == INodeLockType.WRITE_ON_PARENT) && (count[0] + 1 == components.length - 1))
              || (lockType == INodeLockType.WRITE_ON_PARENT && (count[0] + 1 == components.length - 2))) {
        curInodeLock = INodeLockType.WRITE;
      } else if (lockType == INodeLockType.READ_COMMITTED) {
        curInodeLock = INodeLockType.READ_COMMITTED;
      } else {
        curInodeLock = defaultINodeLockType;
      }
      setINodeLockType(curInodeLock);

      lastComp = INodeUtil.getNextChild(
              curNode,
              components,
              count,
              resolveLink,
              true);

      if (curNode[0] != null) {
        addLockedINodes(curNode[0], curInodeLock);
        if (SubtreeLockHelper.isSubtreeLocked(curNode[0].isSubtreeLocked(), curNode[0].getSubtreeLockOwner(), activeNamenodes)) {
          if (!ignoreLocalSubtreeLocks
                  || ignoreLocalSubtreeLocks && namenodeId != curNode[0].getSubtreeLockOwner()) {
            throw new SubtreeLockedException();
          }
        }
        resolvedInodes.add(curNode[0]);
      }

      if (lastComp) {
        break;
      }
    }

// lock upgrade if the path was not fully resolved
    if (resolvedInodes.size() != components.length) { // path was not fully resolved
      INode inodeToReread = null;
      if (lockType == INodeLockType.WRITE_ON_PARENT) {
        if (resolvedInodes.size() <= components.length - 2) {
          inodeToReread = resolvedInodes.get(resolvedInodes.size() - 1);
        }
      } else if (lockType == INodeLockType.WRITE) {
        inodeToReread = resolvedInodes.get(resolvedInodes.size() - 1);
      }

      if (inodeToReread != null) {
        INode inode = find(lockType, inodeToReread.getLocalName(), inodeToReread.getParentId());
        if (inode != null) { // re-read after taking write lock to make sure that no one has created the same inode. 
          addLockedINodes(inode, lockType);
          String existingPath = buildPath(path, resolvedInodes.size());
          List<INode> rest = acquireLockOnRestOfPath(lockType, inode, path, existingPath, false);
          resolvedInodes.addAll(rest);
        }
      }
    }
    return resolvedInodes;
  }

  private List<INode> acquireLockOnRestOfPath(INodeLockType lock, INode baseInode, String fullPath, String prefix, boolean resolveLink)
          throws PersistanceException, UnresolvedPathException {
    List<INode> resolved = new ArrayList<INode>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    int[] count = new int[]{prefixComps.length - 1};
    boolean lastComp;
    INode[] curInode = new INode[]{baseInode};
    while (count[0] < fullComps.length && curInode[0] != null) {
      setINodeLockType(lock);
      lastComp = INodeUtil.getNextChild(
              curInode,
              fullComps,
              count,
              resolveLink,
              true);
      if (curInode[0] != null) {
        addLockedINodes(curInode[0], lock);
        resolved.add(curInode[0]);
      }
      if (lastComp) {
        break;
      }
    }

    return resolved;
  }

  private List<INode> findImmediateChildren(INode lastINode) throws PersistanceException {
    List<INode> children = new ArrayList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        setINodeLockType(lockType);
        children.addAll(((INodeDirectory) lastINode).getChildren());
      }
    }
    return children;
  }

  private Queue<INode> findChildrenRecursively(INode lastINode) throws PersistanceException {
    Queue<INode> children = new LinkedList<INode>();
    Queue<INode> unCheckedDirs = new LinkedList<INode>();
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
    LOG.debug("Added " + children.size() + " childern.");
    return children;
  }

  private INode acquireLockOnRoot(INodeLockType lock) throws PersistanceException {
    LOG.debug("Acquring " + lock + " on the root node");
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

  private void acquireINodeAttributes() throws PersistanceException {
    List<HopINodeCandidatePK> pks = new ArrayList<HopINodeCandidatePK>();
    for (List<INode> resolvedINodes : getAllResolvedINodes()) {
      for (INode inode : resolvedINodes) {
        if (inode instanceof INodeDirectoryWithQuota) {
          HopINodeCandidatePK pk = new HopINodeCandidatePK(inode.getId());
          pks.add(pk);
        }
      }
    }
    acquireLockList(DEFAULT_LOCK_TYPE, INodeAttributes.Finder.ByPKList, pks);
  }
  
}
