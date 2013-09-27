package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockTypes;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.namenode.persistance.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.PendingBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LockUpgradeException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockContext extends EntityContext<PendingBlockInfo> {

  private Map<Long, PendingBlockInfo> pendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> newPendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> modifiedPendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> removedPendings = new HashMap<Long, PendingBlockInfo>();
  private boolean allPendingRead = false;
  private PendingBlockDataAccess dataAccess;

  public PendingBlockContext(PendingBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    newPendings.put(pendingBlock.getBlockId(), pendingBlock);
    log("added-pending", CacheHitState.NA,
            new String[]{"bid", Long.toString(pendingBlock.getBlockId()),
              "numInProgress", Integer.toString(pendingBlock.getNumReplicas())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    pendings.clear();
    newPendings.clear();
    modifiedPendings.clear();
    removedPendings.clear();
    allPendingRead = false;
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<PendingBlockInfo> findList(FinderType<PendingBlockInfo> finder, Object... params) throws PersistanceException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    List<PendingBlockInfo> result = null;
    switch (pFinder) {
      case ByTimeLimit:
        long timeLimit = (Long) params[0];
        log("find-pendings-by-timelimit", CacheHitState.NA, new String[]{"timelimit", Long.toString(timeLimit)});
        aboutToAccessStorage();
        return syncInstances(dataAccess.findByTimeLimitLessThan(timeLimit));
      case All:
        if (allPendingRead) {
          log("find-all-pendings", CacheHitState.HIT);
        } else {
          log("find-all-pendings", CacheHitState.LOSS);
          aboutToAccessStorage();
          syncInstances(dataAccess.findAll());
          allPendingRead = true;
        }
        result = new ArrayList();
        for (PendingBlockInfo pendingBlockInfo : pendings.values()) {
          if (pendingBlockInfo != null) {
            result.add(pendingBlockInfo);
          }
        }
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public PendingBlockInfo find(FinderType<PendingBlockInfo> finder, Object... params) throws PersistanceException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    PendingBlockInfo result = null;
    switch (pFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        if (this.pendings.containsKey(blockId)) {
          log("find-pending-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId)});
          result = this.pendings.get(blockId);
        } else if (!this.removedPendings.containsKey(blockId)) {
          log("find-pending-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId)});
          aboutToAccessStorage();
          result = dataAccess.findByPKey(blockId);
          this.pendings.put(blockId, result);
        } 
        //else {
        //  throw new IllegalStateException("Illegal Cache State");
        //}
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

    @Override
    public void prepare(TransactionLocks tl) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions

        if ((removedPendings.values().size() != 0
                || modifiedPendings.values().size() != 0)
                && tl.getPbLock()!= TransactionLockTypes.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade pending block locks");
        }
        dataAccess.prepare(removedPendings.values(), newPendings.values(), modifiedPendings.values());
    }

  @Override
  public void remove(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (!pendings.containsKey(pendingBlock.getBlockId())) {  
      throw new TransactionContextException("Unattached pending-block passed to be removed id "+pendingBlock.getBlockId());
    }
    pendings.remove(pendingBlock.getBlockId());
    newPendings.remove(pendingBlock.getBlockId());
    modifiedPendings.remove(pendingBlock.getBlockId());
    removedPendings.put(pendingBlock.getBlockId(), pendingBlock);
    log("removed-pending", CacheHitState.NA, new String[]{"bid", Long.toString(pendingBlock.getBlockId())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    modifiedPendings.put(pendingBlock.getBlockId(), pendingBlock);
    log("updated-pending", CacheHitState.NA,
            new String[]{"bid", Long.toString(pendingBlock.getBlockId()),
              "numInProgress", Integer.toString(pendingBlock.getNumReplicas())});
  }

  /**
   * This method only returns the result fetched from the storage not those in
   * the memory.
   *
   * @param pendingTables
   * @return
   */
  private List<PendingBlockInfo> syncInstances(Collection<PendingBlockInfo> pendingTables) {
    List<PendingBlockInfo> newPBlocks = new ArrayList<PendingBlockInfo>();
    for (PendingBlockInfo p : pendingTables) {
      if (pendings.containsKey(p.getBlockId())) {
        if (pendings.get(p.getBlockId()) == null) {
          pendings.put(p.getBlockId(), p);
        }
        newPBlocks.add(pendings.get(p.getBlockId()));
      } else if (!removedPendings.containsKey(p.getBlockId())) {
        pendings.put(p.getBlockId(), p);
        newPBlocks.add(p);
      }
    }

    return newPBlocks;
  }
}
