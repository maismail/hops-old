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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
final class HopsRenameINodeLock extends HopsINodeLock {

  private static final Comparator PATH_COMPARTOR = new Comparator<String>() {
    @Override
    public int compare(String o1, String o2) {
      String[] o1Path = INode.getPathNames(o1);
      String[] o2Path = INode.getPathNames(o2);
      if (o1Path.length > o2Path.length) {
        return 1;
      } else if (o1Path.length == o2Path.length) {
        return o1.compareTo(o2);
      }
      return -1;
    }
  };
  private final boolean legacyRename;

  public HopsRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      boolean ignoreLocalSubtreeLocks, long namenodeId,
      Collection<ActiveNamenode> activeNamenodes, String src, String dst,
      boolean legacyRename) {
    super(lockType, resolveType, false, ignoreLocalSubtreeLocks, namenodeId,
        activeNamenodes, src, dst);
    this.legacyRename = legacyRename;
  }

  public HopsRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      Collection<ActiveNamenode> activeNamenodes, String src, String dst,
      boolean legacyRename) {
    super(lockType, resolveType, false, activeNamenodes, src, dst);
    this.legacyRename = legacyRename;
  }

  public HopsRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      boolean ignoreLocalSubtreeLocks, long namenodeId,
      List<ActiveNamenode> activeNamenodes, String src, String dst) {
    this(lockType, resolveType, ignoreLocalSubtreeLocks, namenodeId,
        activeNamenodes, src, dst, false);
  }

  public HopsRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      Collection<ActiveNamenode> activeNamenodes, String src, String dst) {
    this(lockType, resolveType, activeNamenodes, src, dst, false);
  }

  @Override
  void acquire(TransactionLocks locks) throws Exception {
    //[S] consider src = /a/b/c and dst = /d
    //during the acquire lock of src write locks will be acquired on parent of c and c
    //during the acquire lock of dst write lock on the root will be acquired but the snapshot 
    //layer will not let the request go to the db as it has already cached the root inode
    //one simple solution is that to acquire lock on the short path first
    //setPartitioningKey(PathMemcache.getInstance().getPartitionKey(locks.getInodeParam()[0]));
    String dst = paths[1];
    Arrays.sort(paths, PATH_COMPARTOR);
    acquireInodeLocks();

    if (legacyRename) // In deprecated rename, it allows to move a dir to an existing destination.
    {
      List<INode> dstINodes = getPathINodes(dst);
      String[] dstComponents = INode.getPathNames(dst);
      INode lastComp = dstINodes.get(dstINodes.size() - 1);

      if (dstINodes.size() == dstComponents.length && lastComp.isDirectory()) {
        //the dst exist and is a directory.
        find(dstComponents[dstComponents.length - 1], lastComp.getId());
      }
    }
  }
}
