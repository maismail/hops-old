package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import org.apache.log4j.NDC;
import se.sics.hop.Common;
import se.sics.hop.exception.StorageCallPreventedException;
import static se.sics.hop.metadata.hdfs.entity.EntityContext.currentLockMode;
import static se.sics.hop.metadata.hdfs.entity.EntityContext.log;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class INodeContext extends EntityContext<INode> {

  /**
   * Mappings
   */
  protected Map<Integer, INode> inodesIdIndex = new HashMap<Integer, INode>();
  protected Map<String, INode> inodesNameParentIndex = new HashMap<String, INode>();
  protected Map<Integer, List<INode>> inodesParentIndex = new HashMap<Integer, List<INode>>();
  protected Map<Integer, INode> newInodes = new HashMap<Integer, INode>();
  protected Map<Integer, INode> modifiedInodes = new HashMap<Integer, INode>();
  protected Map<Integer, INode> removedInodes = new HashMap<Integer, INode>();
  protected INodeDataAccess<INode> dataAccess;

  public INodeContext(INodeDataAccess<INode> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(INode inode) throws PersistanceException {
    if (removedInodes.containsKey(inode.getId()) || modifiedInodes.containsKey(inode.getId())) {
      log("added-removed-inode", CacheHitState.NA,
              new String[]{"id", Integer.toString(inode.getId()), "name", inode.getLocalName(),
        "pid", Integer.toString(inode.getParentId())});
      removedInodes.remove(inode.getId());
      update(inode);
    } else {
      inodesIdIndex.put(inode.getId(), inode);
      inodesNameParentIndex.put(inode.nameParentKey(), inode);
      newInodes.put(inode.getId(), inode);
      log("added-inode", CacheHitState.NA,
              new String[]{"id", Integer.toString(inode.getId()), "name", inode.getLocalName(),
        "pid", Integer.toString(inode.getParentId())});
    }
  }

  @Override
  public int count(CounterType<INode> counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    inodesIdIndex.clear();
    inodesNameParentIndex.clear();
    inodesParentIndex.clear();
    removedInodes.clear();
    newInodes.clear();
    modifiedInodes.clear();
  }

  @Override
  public INode find(FinderType<INode> finder, Object... params) throws PersistanceException {
    INode.Finder iFinder = (INode.Finder) finder;
    INode result = null;
    switch (iFinder) {
      case ByINodeID:
        Integer inodeId = (Integer) params[0];
        if (removedInodes.containsKey(inodeId)) {
          log("find-inode-by-pk-removed", CacheHitState.HIT, new String[]{"id", Integer.toString(inodeId)});
          result = null;
        } else if (inodesIdIndex.containsKey(inodeId)) {
          log("find-inode-by-pk", CacheHitState.HIT, new String[]{"id", Integer.toString(inodeId)});
          result = inodesIdIndex.get(inodeId);
        } else {
          log("find-inode-by-pk", CacheHitState.LOSS, new String[]{"id", Integer.toString(inodeId)});
          aboutToAccessStorage();
          result = dataAccess.indexScanfindInodeById(inodeId);
          inodesIdIndex.put(inodeId, result);
          if (result != null) {
            inodesNameParentIndex.put(result.nameParentKey(), result);
          }
        }
        break;
      case ByPK_NameAndParentId:
        String name = (String) params[0];
        Integer parentId = (Integer) params[1];
        String key = parentId + name;
        if (inodesNameParentIndex.containsKey(key)) {
          result = inodesNameParentIndex.get(key);
          if (!preventStorageCalls() && (currentLockMode.get() == LockMode.WRITE_LOCK)) {
            //trying to upgrade lock. re-read the row from DB
            log("find-inode-by-name-parentid-LOCK_UPGRADE", CacheHitState.LOSS_LOCK_UPGRADE,
                    new String[]{"name", name, "pid", Integer.toString(parentId)});
            aboutToAccessStorage(getClass().getSimpleName() + " findInodeByNameAndParentId. name " + name + " parent_id " + parentId);
            result = dataAccess.pkLookUpFindInodeByNameAndParentId(name, parentId);
            if(result!=null){
              inodesIdIndex.put(result.getId(), result);
            }
            inodesNameParentIndex.put(key, result);
          } else {
            log("find-inode-by-name-parentid", CacheHitState.HIT,
                    new String[]{"name", name, "pid", Integer.toString(parentId)});
          }

        } else if (newInodes.containsKey(parentId)) {
          log("find-inode-by-name-new-parentid", CacheHitState.HIT,
                  new String[]{"name", name, "pid", Integer.toString(parentId)});
          result = null;
        } else if (isRemoved(parentId, name)) {
          return result; // return null; the node was remove. 
        } else {
          aboutToAccessStorage(getClass().getSimpleName() + " findInodeByNameAndParentId. name " + name + " parent_id " + parentId);
          result = dataAccess.pkLookUpFindInodeByNameAndParentId(name, parentId);
          if(result!=null){
            inodesIdIndex.put(result.getId(), result);
          }
          inodesNameParentIndex.put(key, result);
          log("find-inode-by-name-parentid", CacheHitState.LOSS, new String[]{"name", name, "pid", Integer.toString(parentId)});
        }
        break;
    }

    return result;
  }
  
  void addINodeToCache(INode inode){
    
  }

  @Override
  public Collection<INode> findList(FinderType<INode> finder, Object... params) throws PersistanceException {
    INode.Finder iFinder = (INode.Finder) finder;
    List<INode> result = null;
    switch (iFinder) {
      case ParentId:
        Integer parentId = (Integer) params[0];
        if (inodesParentIndex.containsKey(parentId)) {
          log("find-inodes-by-parentid", CacheHitState.HIT, new String[]{"pid", Integer.toString(parentId)});
          result = inodesParentIndex.get(parentId);
        } else {
          log("find-inodes-by-parentid", CacheHitState.LOSS, new String[]{"pid", Integer.toString(parentId)});
          aboutToAccessStorage();
          result = syncInodeInstances(dataAccess.indexScanFindInodesByParentId(parentId));
          Collections.sort(result, INode.Order.ByName);
          inodesParentIndex.put(parentId, result);
        }
        break;
    }
    return result;
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    // if the list is not empty then check for the lock types
    // lock type is checked after when list lenght is checked 
    // because some times in the tx handler the acquire lock 
    // function is empty and in that case tlm will throw 
    // null pointer exceptions
    HDFSTransactionLocks hlks = (HDFSTransactionLocks) lks;
    if (!removedInodes.values().isEmpty()) {
      for (INode inode : removedInodes.values()) {
        INodeLockType lock = hlks.getLockedINodeLockType(inode);
        if (lock != null && lock != INodeLockType.WRITE && lock != INodeLockType.WRITE_ON_PARENT) {
          throw new LockUpgradeException("Trying to remove inode id=" + inode.getId() + " acquired lock was " + lock);
        }
      }
    }

    if (!modifiedInodes.values().isEmpty()) {
      for (INode inode : modifiedInodes.values()) {
        INodeLockType lock = hlks.getLockedINodeLockType(inode);
        if (lock != null && lock != INodeLockType.WRITE && lock != INodeLockType.WRITE_ON_PARENT) {
          throw new LockUpgradeException("Trying to update inode id=" + inode.getId() + " acquired lock was " + lock);
        }
      }
    }
    dataAccess.prepare(removedInodes.values(), newInodes.values(), modifiedInodes.values());
  }

  @Override
  public void remove(INode inode) throws PersistanceException {
    inodesIdIndex.remove(inode.getId());
    inodesNameParentIndex.remove(inode.nameParentKey());
    newInodes.remove(inode.getId());
    modifiedInodes.remove(inode.getId());
    removedInodes.put(inode.getId(), inode);
    log("removed-inode", CacheHitState.NA, new String[]{"id", Integer.toString(inode.getId()), "name", inode.getLocalName()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(INode inode) throws PersistanceException {

    if (removedInodes.containsKey(inode.getId())) {
      throw new TransactionContextException("Removed  inode passed to be persisted. NDC peek " + NDC.peek());
    }

    inodesIdIndex.put(inode.getId(), inode);
    inodesNameParentIndex.put(inode.nameParentKey(), inode);
    //if in new list then update in the new list
    if (newInodes.containsKey(inode.getId())) {
      newInodes.put(inode.getId(), inode);
    } else {
      modifiedInodes.put(inode.getId(), inode);
    }

    log("updated-inode", CacheHitState.NA, new String[]{"id", Integer.toString(inode.getId()), "name", inode.getLocalName()});
  }

  private List<INode> syncInodeInstances(List<INode> newInodes) {
    List<INode> finalList = new ArrayList<INode>();

    for (INode inode : newInodes) {
      if (removedInodes.containsKey(inode.getId())) {
        continue;
      }
      if (inodesIdIndex.containsKey(inode.getId())) {
        if (inodesIdIndex.get(inode.getId()) == null) {
          inodesIdIndex.put(inode.getId(), inode);
        }
        finalList.add(inodesIdIndex.get(inode.getId()));
      } else {
        inodesIdIndex.put(inode.getId(), inode);
        finalList.add(inode);
      }

      String key = inode.nameParentKey();
      if (inodesNameParentIndex.containsKey(key)) {
        if (inodesNameParentIndex.get(key) == null) {
          inodesNameParentIndex.put(key, inode);
        }

      } else {
        inodesNameParentIndex.put(key, inode);
      }
    }

    return finalList;
  }

  private boolean isRemoved(final Integer parent_id, final String name) {
    for (INode inode : removedInodes.values()) {
      if (inode.getParentId() == parent_id
              && inode.getLocalName().equals(name)) {
        return true;
      }
    }

    return false;
  }

  private boolean isRemoved(final Integer id) {
    for (INode inode : removedInodes.values()) {
      if (inode.getId() == id) {
        return true;
      }
    }

    return false;
  }

  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("INode Context", newInodes.size(), modifiedInodes.size(), removedInodes.size());
    return stat;
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params) throws PersistanceException {
    HOPTransactionContextMaintenanceCmds hopCmds = (HOPTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        //delete the previous row from db
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        removedInodes.put(inodeBeforeChange.getId(), inodeBeforeChange);
        log("snapshot-maintenance-inode-pk-change", CacheHitState.NA, new String[]{"Before inodeId", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId()), "After inodeId", Integer.toString(inodeAfterChange.getId()), "name", inodeAfterChange.getLocalName(), "pid", Integer.toString(inodeAfterChange.getParentId())});
        log("snapshot-maintenance-removed-inode", CacheHitState.NA, new String[]{"name", inodeBeforeChange.getLocalName(), "inodeId", Integer.toString(inodeBeforeChange.getId()), "pid", Integer.toString(inodeBeforeChange.getParentId())});
        break;
      case Concat:
        // do nothing
        // why? files y and z are merged into file x. 
        // all the blocks will be added to file x and the inodes y and z will be deleted.
        // Inode deletion is handled by the concat function
        break;
    }
  }
}
