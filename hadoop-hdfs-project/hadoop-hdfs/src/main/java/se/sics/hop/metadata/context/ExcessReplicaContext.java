package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaContext extends EntityContext<HopExcessReplica> {

  private Map<HopExcessReplica, HopExcessReplica> exReplicas = new HashMap<HopExcessReplica, HopExcessReplica>();
  private Map<Long, TreeSet<HopExcessReplica>> blockIdToExReplica = new HashMap<Long, TreeSet<HopExcessReplica>>();
  private Map<String, TreeSet<HopExcessReplica>> storageIdToExReplica = new HashMap<String, TreeSet<HopExcessReplica>>();
  private Map<HopExcessReplica, HopExcessReplica> newExReplica = new HashMap<HopExcessReplica, HopExcessReplica>();
  private Map<HopExcessReplica, HopExcessReplica> removedExReplica = new HashMap<HopExcessReplica, HopExcessReplica>();
  private ExcessReplicaDataAccess<HopExcessReplica>  dataAccess;
  private int nullCount = 0;

  public ExcessReplicaContext(ExcessReplicaDataAccess<HopExcessReplica>  dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopExcessReplica exReplica) throws TransactionContextException {
    if (removedExReplica.containsKey(exReplica)) {
      throw new TransactionContextException("Removed excess-replica passed to be persisted");
    }

    if (exReplicas.containsKey(exReplica) && exReplicas.get(exReplica) == null) {
      nullCount--;
    }

    exReplicas.put(exReplica, exReplica);
    newExReplica.put(exReplica, exReplica);
    log("added-excess", CacheHitState.NA,
            new String[]{"bid", Long.toString(exReplica.getBlockId()), "sid", exReplica.getStorageId()});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    exReplicas.clear();
    storageIdToExReplica.clear();
    blockIdToExReplica.clear();
    newExReplica.clear();
    removedExReplica.clear();
    nullCount = 0;
  }

  @Override
  public int count(CounterType<HopExcessReplica> counter, Object... params) throws PersistanceException {
    HopExcessReplica.Counter eCounter = (HopExcessReplica.Counter) counter;
    switch (eCounter) {
      case All:
        log("count-all-excess");
        return dataAccess.countAll() + newExReplica.size() - removedExReplica.size() - nullCount;
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public HopExcessReplica find(FinderType<HopExcessReplica> finder,
          Object... params) throws PersistanceException {
    HopExcessReplica.Finder eFinder = (HopExcessReplica.Finder) finder;
    HopExcessReplica result = null;

    switch (eFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        String storageId = (String) params[1];
        HopExcessReplica searchKey = new HopExcessReplica(storageId, blockId);
        if (blockIdToExReplica.containsKey(blockId) && !blockIdToExReplica.get(blockId).contains(searchKey)) {
          log("find-excess-by-pk-not-exist", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return null;
        }
        if (exReplicas.containsKey(searchKey)) {
          log("find-excess-by-pk", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", storageId});
          result = exReplicas.get(searchKey);
        } else if (removedExReplica.containsKey(searchKey)) {
          log("find-excess-by-pk-removed-item", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", storageId});
          result = null;
        } else {
          log("find-excess-by-pk", CacheHitState.LOSS,
                  new String[]{"bid", Long.toString(blockId), "sid", storageId});
          aboutToAccessStorage();
          result = dataAccess.findByPkey(params);
          if (result == null) {
            nullCount++;
          }
          this.exReplicas.put(result, result);
        }
        return result;
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopExcessReplica> findList(FinderType<HopExcessReplica> finder, Object... params) throws PersistanceException {
    HopExcessReplica.Finder eFinder = (HopExcessReplica.Finder) finder;
    TreeSet<HopExcessReplica> result = null;

    switch (eFinder) {
      case ByStorageId:
        String sId = (String) params[0];
        if (storageIdToExReplica.containsKey(sId)) {
          log("find-excess-by-storageid", CacheHitState.HIT, new String[]{"sid", sId});
        } else {
          log("find-excess-by-storageid", CacheHitState.LOSS, new String[]{"sid", sId});
          aboutToAccessStorage();
          TreeSet<HopExcessReplica> syncSet = syncExcessReplicaInstances(dataAccess.findExcessReplicaByStorageId(sId));
          storageIdToExReplica.put(sId, syncSet);
        }
        result = storageIdToExReplica.get(sId);
        return result;
      case ByBlockId:
        long bId = (Long) params[0];
        if (blockIdToExReplica.containsKey(bId)) {
          log("find-excess-by-blockId", CacheHitState.HIT, new String[]{"bid", String.valueOf(bId)});
        } else {
          log("find-excess-by-blockId", CacheHitState.LOSS, new String[]{"bid", String.valueOf(bId)});
          aboutToAccessStorage();
          TreeSet<HopExcessReplica> syncSet = syncExcessReplicaInstances(dataAccess.findExcessReplicaByBlockId(bId));
          blockIdToExReplica.put(bId, syncSet);
        }
        result = blockIdToExReplica.get(bId);
        return result;
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

    @Override
    public void prepare(TransactionLocks lks) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions
        HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
        if ((!removedExReplica.values().isEmpty())
                && hlks.getErLock() != TransactionLockTypes.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade block locks");
        }
        dataAccess.prepare(removedExReplica.values(), newExReplica.values(), null);
    }

  @Override
  public void remove(HopExcessReplica exReplica) throws PersistanceException {
    if (exReplicas.remove(exReplica) == null) {
      throw new TransactionContextException("Unattached excess-replica passed to be removed");
    }

    newExReplica.remove(exReplica);
    removedExReplica.put(exReplica, exReplica);
    log("removed-excess", CacheHitState.NA,
            new String[]{"bid", Long.toString(exReplica.getBlockId()), "sid", exReplica.getStorageId()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopExcessReplica entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private TreeSet<HopExcessReplica> syncExcessReplicaInstances(List<HopExcessReplica> list) {
    TreeSet<HopExcessReplica> replicaSet = new TreeSet<HopExcessReplica>();

    for (HopExcessReplica replica : list) {
      if (!removedExReplica.containsKey(replica)) {
        if (exReplicas.containsKey(replica)) {
          if (exReplicas.get(replica) == null) {
            exReplicas.put(replica, replica);
            nullCount--;
          }
          replicaSet.add(exReplicas.get(replica));
        } else {
          exReplicas.put(replica, replica);
          replicaSet.add(replica);
        }
      }
    }

    return replicaSet;
  }
}