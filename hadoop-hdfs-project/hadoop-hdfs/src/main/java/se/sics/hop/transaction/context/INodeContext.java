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
package se.sics.hop.transaction.context;

import com.google.common.base.Predicate;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.transaction.lock.HopsBaseINodeLock;
import se.sics.hop.transaction.lock.HopsLock;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class INodeContext extends BaseEntityContext<Integer, INode> {

  private final INodeDataAccess<INode> dataAccess;

  private final Map<String, INode> inodesNameParentIndex =
      new HashMap<String, INode>();
  private final Map<Integer, List<INode>> inodesParentIndex =
      new HashMap<Integer, List<INode>>();
  private final List<INode> renamedInodes = new ArrayList<INode>();

  public INodeContext(INodeDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodesNameParentIndex.clear();
    inodesParentIndex.clear();
    renamedInodes.clear();
  }

  @Override
  public INode find(FinderType<INode> finder, Object... params)
      throws TransactionContextException, StorageException {
    INode.Finder iFinder = (INode.Finder) finder;
    switch (iFinder) {
      case ByINodeID:
        return findByInodeId(params);
      case ByPK_NameAndParentId:
        return findByNameAndParentId(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<INode> findList(FinderType<INode> finder, Object... params)
      throws TransactionContextException, StorageException {
    INode.Finder iFinder = (INode.Finder) finder;
    switch (iFinder) {
      case ParentId:
        return findByParentId(params);
      case ByPKS:
        return findBatch(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void remove(INode iNode) throws TransactionContextException {
    super.remove(iNode);
    inodesNameParentIndex.remove(iNode.nameParentKey());
    log("removed-inode", CacheHitState.NA,
        new String[]{"id", Integer.toString(iNode.getId()), "name",
            iNode.getLocalName()});
  }

  @Override
  public void update(INode iNode) throws TransactionContextException {
    super.update(iNode);
    inodesNameParentIndex.put(iNode.nameParentKey(), iNode);
    log("updated-inode", CacheHitState.NA,
        new String[]{"id", Integer.toString(iNode.getId()), "name",
            iNode.getLocalName()});
  }

  @Override
  public void prepare(TransactionLocks lks)
      throws TransactionContextException, StorageException {

    // if the list is not empty then check for the lock types
    // lock type is checked after when list length is checked
    // because some times in the tx handler the acquire lock
    // function is empty and in that case tlm will throw
    // null pointer exceptions
    Collection<INode> removed = getRemoved();
    Collection<INode> added = new ArrayList<INode>(getAdded());
    added.addAll(renamedInodes);
    Collection<INode> modified = getModified();

    if (lks.containsLock(HopsLock.Type.INode)) {
      HopsBaseINodeLock hlk =
          (HopsBaseINodeLock) lks.getLock(HopsLock.Type.INode);
      if (!removed.isEmpty()) {
        for (INode inode : removed) {
          TransactionLockTypes.INodeLockType lock =
              hlk.getLockedINodeLockType(inode);
          if (lock != null &&
              lock != TransactionLockTypes.INodeLockType.WRITE && lock !=
              TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT) {
            throw new LockUpgradeException(
                "Trying to remove inode id=" + inode.getId() +
                    " acquired lock was " + lock);
          }
        }
      }

      if (!modified.isEmpty()) {
        for (INode inode : modified) {
          TransactionLockTypes.INodeLockType lock =
              hlk.getLockedINodeLockType(inode);
          if (lock != null &&
              lock != TransactionLockTypes.INodeLockType.WRITE && lock !=
              TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT) {
            throw new LockUpgradeException(
                "Trying to update inode id=" + inode.getId() +
                    " acquired lock was " + lock);
          }
        }
      }
    }


    dataAccess.prepare(removed, added, modified);
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HOPTransactionContextMaintenanceCmds hopCmds =
        (HOPTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        //delete the previous row from db
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        super.remove(inodeBeforeChange);
        renamedInodes.add(inodeAfterChange);
        log("snapshot-maintenance-inode-pk-change", CacheHitState.NA,
            new String[]{"Before inodeId",
                Integer.toString(inodeBeforeChange.getId()), "name",
                inodeBeforeChange.getLocalName(), "pid",
                Integer.toString(inodeBeforeChange.getParentId()),
                "After inodeId", Integer.toString(inodeAfterChange.getId()),
                "name", inodeAfterChange.getLocalName(), "pid",
                Integer.toString(inodeAfterChange.getParentId())});
        log("snapshot-maintenance-removed-inode", CacheHitState.NA,
            new String[]{"name", inodeBeforeChange.getLocalName(), "inodeId",
                Integer.toString(inodeBeforeChange.getId()), "pid",
                Integer.toString(inodeBeforeChange.getParentId())});
        break;
      case Concat:
        // do nothing
        // why? files y and z are merged into file x.
        // all the blocks will be added to file x and the inodes y and z will be deleted.
        // Inode deletion is handled by the concat function
        break;
    }
  }

  @Override
  Integer getKey(INode iNode) {
    return iNode.getId();
  }


  private INode findByInodeId(Object[] params)
      throws TransactionContextException, StorageException {
    INode result = null;
    final Integer inodeId = (Integer) params[0];
    if (contains(inodeId)) {
      log("find-inode-by-id", CacheHitState.LOSS, new String[]{"id",
          Integer.toString(inodeId)});
      result = get(inodeId);
    } else {
      log("find-inode-by-id", CacheHitState.LOSS, new String[]{"id",
          Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.indexScanfindInodeById(inodeId);
      gotFromDB(inodeId, result);
      if (result != null) {
        inodesNameParentIndex.put(result.nameParentKey(), result);
      }
    }
    return result;
  }

  private INode findByNameAndParentId(Object[] params)
      throws TransactionContextException, StorageException {

    INode result = null;
    final String name = (String) params[0];
    final Integer parentId = (Integer) params[1];
    final String nameParentKey = INode.nameParentKey(parentId, name);

    if (inodesNameParentIndex.containsKey(nameParentKey)) {
      result = inodesNameParentIndex.get(nameParentKey);
      if (!preventStorageCalls() &&
          (currentLockMode.get() == LockMode.WRITE_LOCK)) {
        //trying to upgrade lock. re-read the row from DB
        log("find-inode-by-name-parentid-LOCK_UPGRADE",
            CacheHitState.LOSS_LOCK_UPGRADE,
            new String[]{"name", name, "pid", Integer.toString(parentId)});
        aboutToAccessStorage();
        result = dataAccess.pkLookUpFindInodeByNameAndParentId(name, parentId);
        gotFromDB(result);
        inodesNameParentIndex.put(nameParentKey, result);
      } else {
        log("find-inode-by-name-parentid", CacheHitState.HIT,
            new String[]{"name", name, "pid", Integer.toString(parentId)});
      }

    } else {
      if (!isNewlyAdded(parentId) && !containsRemoved(parentId,
          name)) {
        aboutToAccessStorage();
        result = dataAccess.pkLookUpFindInodeByNameAndParentId(name, parentId);
        gotFromDB(result);
        inodesNameParentIndex.put(nameParentKey, result);
        log("find-inode-by-name-parentid", CacheHitState.LOSS,
            new String[]{"name", name, "pid", Integer.toString(parentId)});
      }
    }
    return result;
  }

  private List<INode> findByParentId(Object[] params) throws
      TransactionContextException, StorageException {
    final Integer parentId = (Integer) params[0];
    List<INode> result = null;
    if (inodesParentIndex.containsKey(parentId)) {
      log("find-inodes-by-parentid", CacheHitState.HIT,
          new String[]{"pid", Integer.toString(parentId)});
      result = inodesParentIndex.get(parentId);
    } else {
      log("find-inodes-by-parentid", CacheHitState.LOSS,
          new String[]{"pid", Integer.toString(parentId)});
      aboutToAccessStorage();
      result = syncInodeInstances(
          dataAccess.indexScanFindInodesByParentId(parentId));
      inodesParentIndex.put(parentId, result);
    }
    return result;
  }

  private List<INode> findBatch(Object[] params) throws
      TransactionContextException, StorageException {
    final String[] names = (String[]) params[0];
    final int[] parentIds = (int[]) params[1];
    log("find-inodes-by-name-parentid", CacheHitState.LOSS,
        new String[]{"name", Arrays.toString(
            names), "pid", Arrays.toString(parentIds)});
    return syncInodeInstances(dataAccess.getINodesPkBatched
        (names, parentIds));
  }

  private List<INode> syncInodeInstances(List<INode> newInodes) {
    List<INode> finalList = new ArrayList<INode>(newInodes.size());

    for (INode inode : newInodes) {
      if (isRemoved(inode.getId())) {
        continue;
      }

      gotFromDB(inode);
      finalList.add(inode);

      String key = inode.nameParentKey();
      if (inodesNameParentIndex.containsKey(key)) {
        if (inodesNameParentIndex.get(key) == null) {
          inodesNameParentIndex.put(key, inode);
        }
      } else {
        inodesNameParentIndex.put(key, inode);
      }
    }
    Collections.sort(finalList, INode.Order.ByName);
    return finalList;
  }

  private boolean containsRemoved(final Integer parentId, final String name) {
    return contains(new Predicate<ContextEntity>() {
      @Override
      public boolean apply(ContextEntity input) {
        INode iNode = input.getEntity();
        return input.getState() == State.REMOVED && iNode.getParentId() ==
            parentId && iNode.getLocalName().equals(name);
      }
    });
  }
}
