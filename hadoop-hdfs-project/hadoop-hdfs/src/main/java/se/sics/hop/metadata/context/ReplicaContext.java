package se.sics.hop.metadata.context;

import se.sics.hop.metadata.entity.EntityContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import se.sics.hop.metadata.entity.hop.HopIndexedReplica;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.metadata.entity.CounterType;
import se.sics.hop.metadata.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.dal.ReplicaDataAccess;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaContext extends EntityContext<HopIndexedReplica> {

  /**
   * Mappings
   */
  private Map<HopIndexedReplica, HopIndexedReplica> removedReplicas = new HashMap<HopIndexedReplica, HopIndexedReplica>();
  private Map<HopIndexedReplica, HopIndexedReplica> newReplicas = new HashMap<HopIndexedReplica, HopIndexedReplica>();
  private Map<HopIndexedReplica, HopIndexedReplica> modifiedReplicas = new HashMap<HopIndexedReplica, HopIndexedReplica>();
  private Map<Long, List<HopIndexedReplica>> blockReplicas = new HashMap<Long, List<HopIndexedReplica>>();
  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopIndexedReplica replica) throws PersistanceException {
    if (removedReplicas.containsKey(replica)) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    newReplicas.put(replica, replica);
    blockReplicas.get(replica.getBlockId()).add(replica);
    log("added-replica", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", replica.getStorageId(), "index", Integer.toString(replica.getIndex())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    newReplicas.clear();
    modifiedReplicas.clear();
    removedReplicas.clear();
    blockReplicas.clear();
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

    @Override
    public void prepare(TransactionLocks lks) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions
        HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
        if ((!removedReplicas.values().isEmpty()
                || !modifiedReplicas.values().isEmpty())
                && hlks.getReplicaLock() != TransactionLockTypes.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade replica locks");
        }
        dataAccess.prepare(removedReplicas.values(), newReplicas.values(), modifiedReplicas.values());
    }

  @Override
  public void remove(HopIndexedReplica replica) throws PersistanceException {
    modifiedReplicas.remove(replica);
    blockReplicas.get(replica.getBlockId()).remove(replica);
    if (newReplicas.containsKey(replica)) {
      newReplicas.remove(replica);
    } else {
      removedReplicas.put(replica, replica);
    }
    log("removed-replica", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", replica.getStorageId(), "index", Integer.toString(replica.getIndex())});
  }

  @Override
  public HopIndexedReplica find(FinderType<HopIndexedReplica> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<HopIndexedReplica> findList(FinderType<HopIndexedReplica> finder, Object... params) throws PersistanceException {
    HopIndexedReplica.Finder iFinder = (HopIndexedReplica.Finder) finder;
    List<HopIndexedReplica> result = null;

    switch (iFinder) {
      case ByBlockId:
        long id = (Long) params[0];
        if (blockReplicas.containsKey(id)) {
          log("find-replicas-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(id)});
          result = blockReplicas.get(id);
        } else {
          log("find-replicas-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(id)});
          aboutToAccessStorage();
          result = dataAccess.findReplicasById(id);
          blockReplicas.put(id, result);
        }
        return new ArrayList<HopIndexedReplica>(result); // Shallow copy
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopIndexedReplica replica) throws PersistanceException {
    if (removedReplicas.containsKey(replica)) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    modifiedReplicas.put(replica, replica);
    List<HopIndexedReplica> list = blockReplicas.get(replica.getBlockId());
    list.remove(replica);
    list.add(replica);
    log("updated-replica", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", replica.getStorageId(), "index", Integer.toString(replica.getIndex())});
  }
}
