package se.sics.hop.metadata.persistence.context.entity;

import java.util.ArrayList;
import se.sics.hop.metadata.persistence.dal.ReplicaUnderConstructionDataAccess;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes;
import se.sics.hop.metadata.persistence.lock.TransactionLocks;
import se.sics.hop.metadata.persistence.CounterType;
import se.sics.hop.metadata.persistence.FinderType;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.metadata.persistence.context.TransactionContextException;
import se.sics.hop.metadata.persistence.context.LockUpgradeException;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionContext extends EntityContext<ReplicaUnderConstruction> {

  /**
   * Mappings
   */
  private Map<ReplicaUnderConstruction, ReplicaUnderConstruction> newReplicasUc = new HashMap<ReplicaUnderConstruction, ReplicaUnderConstruction>();
  private Map<ReplicaUnderConstruction, ReplicaUnderConstruction> removedReplicasUc = new HashMap<ReplicaUnderConstruction, ReplicaUnderConstruction>();
  private Map<Long, List<ReplicaUnderConstruction>> blockReplicasUCAll = new HashMap<Long, List<ReplicaUnderConstruction>>();
  private ReplicaUnderConstructionDataAccess<ReplicaUnderConstruction> dataAccess;

  public ReplicaUnderConstructionContext(ReplicaUnderConstructionDataAccess<ReplicaUnderConstruction> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(ReplicaUnderConstruction replica) throws PersistanceException {
    if (removedReplicasUc.containsKey(replica)) {
      throw new TransactionContextException("Removed  under constructionreplica passed to be persisted");
    }

    newReplicasUc.put(replica, replica);
    if (replica.getBlockId() == -1) {
      throw new IllegalArgumentException("Block Id is -1");
    }
    if (blockReplicasUCAll.get(replica.getBlockId()) == null) {
      blockReplicasUCAll.put(replica.getBlockId(), new ArrayList<ReplicaUnderConstruction>());
    }
    blockReplicasUCAll.get(replica.getBlockId()).add(replica);

    log("added-replicauc", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", replica.getStorageId(), "state", replica.getState().name()});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    newReplicasUc.clear();
    removedReplicasUc.clear();
    blockReplicasUCAll.clear();
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<ReplicaUnderConstruction> findList(FinderType<ReplicaUnderConstruction> finder, Object... params) throws PersistanceException {

    ReplicaUnderConstruction.Finder rFinder = (ReplicaUnderConstruction.Finder) finder;
    List<ReplicaUnderConstruction> result = null;
    switch (rFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        if (blockReplicasUCAll.containsKey(blockId)) {
          log("find-replicaucs-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId)});
          result = blockReplicasUCAll.get(blockId);
        } else {
          if (isTxRunning()) // if Tx is running and we dont have the data in the cache then it means it was null 
          {
            return result;
          }
          log("find-replicaucs-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId)});
          aboutToAccessStorage();
          result = dataAccess.findReplicaUnderConstructionByBlockId(blockId);
          blockReplicasUCAll.put(blockId, result);
        }
        break;
    }

    return result;
  }

  @Override
  public ReplicaUnderConstruction find(FinderType<ReplicaUnderConstruction> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    // if the list is not empty then check for the lock types
    // lock type is checked after when list lenght is checked 
    // because some times in the tx handler the acquire lock 
    // function is empty and in that case tlm will throw 
    // null pointer exceptions

    if ((removedReplicasUc.values().size() != 0)
            && lks.getRucLock() != TransactionLockTypes.LockType.WRITE) {
      throw new LockUpgradeException("Trying to upgrade replica under construction locks");
    }
    dataAccess.prepare(removedReplicasUc.values(), newReplicasUc.values(), null);
  }

  @Override
  public void remove(ReplicaUnderConstruction replica) throws PersistanceException {

    boolean removed = false;
    if (blockReplicasUCAll.containsKey(replica.getBlockId())) {
      List<ReplicaUnderConstruction> urbs = blockReplicasUCAll.get(replica.getBlockId());
      if (urbs.contains(replica)) {
        removedReplicasUc.put(replica, replica);
        blockReplicasUCAll.remove(replica);
        removed = true;
      }
    }
    if (!removed) {

      throw new StorageException("Trying to delete row in ruc table that was not locked. ruc bid " + replica.getBlockId()
              + " sid " + replica.getStorageId());
    }
    newReplicasUc.remove(replica);
    log("removed-replicauc", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", replica.getStorageId(), "state", replica.getState().name(),
      " replicas to be removed", Integer.toString(removedReplicasUc.size()),
      "Storage id", replica.getStorageId()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void update(ReplicaUnderConstruction replica) throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }
}
