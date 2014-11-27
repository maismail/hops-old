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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Iterables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.transaction.EntityManager;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public abstract class HopsBaseINodeLock extends HopsLock {

  protected final static Log LOG = LogFactory.getLog(HopsBaseINodeLock.class);
  private static boolean setPartitionKeyEnabled = false;

  private final Map<INode, TransactionLockTypes.INodeLockType> allLockedInodesInTx;
  private final ResolvedINodesMap resolvedINodesMap;

  public static class ResolvedINodesMap {
    private Map<String, PathINodes> pathToPathINodes = new TreeMap<String, PathINodes>();
    private final Collection<INode> individualInodes = new ArrayList<INode>();

    public static class PathINodes {
      private List<INode> pathINodes;
      private List<INode> childINodes;

      public List<INode> getPathINodes() {
        return pathINodes;
      }

      public void setPathINodes(List<INode> pathINodes) {
        this.pathINodes = pathINodes;
      }

      public List<INode> getChildINodes() {
        return childINodes;
      }

      public void setChildINodes(List<INode> childINodes) {
        this.childINodes = childINodes;
      }
    }

    private PathINodes getWithLazyInit(String path) {
      if (!pathToPathINodes.containsKey(path)) {
        return pathToPathINodes.put(path, new PathINodes());
      }
      return pathToPathINodes.get(path);
    }

    public void putPathINodes(String path, List<INode> iNodes) {
      PathINodes pathINodes = getWithLazyInit(path);
      pathINodes.setPathINodes(iNodes);
    }

    public void putChildINodes(String path, List<INode> iNodes) {
      PathINodes pathINodes = getWithLazyInit(path);
      pathINodes.setChildINodes(iNodes);
    }

    public void putIndividualINode(INode iNode) {
      individualInodes.add(iNode);
    }

    public List<INode> getPathINodes(String path) {
      return pathToPathINodes.get(path).getPathINodes();
    }

    public List<INode> getChildINodes(String path) {
      return pathToPathINodes.get(path).getPathINodes();
    }

    public Iterable<INode> getAll() {
      Iterable iterable = null;
      for (PathINodes pathINodes : pathToPathINodes.values()) {
        if (iterable == null) {
          iterable = Iterables.concat(pathINodes.getPathINodes(),
              pathINodes.getChildINodes());
        } else {
          iterable = Iterables.concat(iterable, pathINodes.getPathINodes(),
              pathINodes.getChildINodes());
        }
      }
      return Iterables.concat(iterable, individualInodes);
    }
  }

  public HopsBaseINodeLock() {
    this.allLockedInodesInTx = new HashMap<INode, TransactionLockTypes.INodeLockType>();
    this.resolvedINodesMap = new ResolvedINodesMap();
  }

  public static void enableSetPartitionKey() {
    setPartitionKeyEnabled = true;
  }

  public static void disableSetPartitionKey() {
    setPartitionKeyEnabled = false;
  }

  public TransactionLockTypes.INodeLockType getLockedINodeLockType(INode inode) {
    return allLockedInodesInTx.get(inode);
  }

  protected INode find(TransactionLockTypes.INodeLockType lock, String name, int parentId) throws PersistanceException {
    setINodeLockType(lock);
    INode inode = EntityManager.find(INode.Finder.ByPK_NameAndParentId, name, parentId);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected Collection<INode> find(
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

  protected INode find(
          TransactionLockTypes.INodeLockType lock,
          int id)
          throws PersistanceException {
    setINodeLockType(lock);
    INode inode = EntityManager.find(INode.Finder.ByINodeID, id);
    addLockedINodes(inode, lock);
    return inode;
  }

  protected void addLockedINodes(
          INode inode, TransactionLockTypes.INodeLockType lock) {
    if (inode == null) {
      return;
    }
    TransactionLockTypes.INodeLockType oldLock = allLockedInodesInTx.get(inode);
    if (oldLock == null || oldLock.compareTo(lock) < 0) {
      allLockedInodesInTx.put(inode, lock);
    }
  }

  protected void setINodeLockType(TransactionLockTypes.INodeLockType lock) throws StorageException {
    switch (lock) {
      case WRITE:
      case WRITE_ON_PARENT:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITTED:
        EntityManager.readCommited();
        break;
    }
  }

  //TODO check if it does work or not and connect it to memchached
  // or use memcached to balance the reading by starting from reverse order of the path
  protected void setPartitionKey() {
    //      setPartitioningKey(PathMemcache.getInstance().getPartitionKey(locks.getInodeParam()[0]));
  }

  protected void setPartitioningKey(Integer inodeId) throws StorageException {
    if (inodeId == null || !setPartitionKeyEnabled) {
      LOG.warn("Transaction Partition Key is not Set");
    } else {
      //set partitioning key
      Object[] key = new Object[2];
      key[0] = inodeId;
      key[1] = new Long(0);

      EntityManager.setPartitionKey(BlockInfoDataAccess.class, key);
      LOG.debug("Setting Partitioning Key to be " + inodeId);
    }
  }

  @Override
  Type getType() {
    return Type.INode;
  }

  public ResolvedINodesMap getResolvedINodesMap() {
    return resolvedINodesMap;
  }
}
