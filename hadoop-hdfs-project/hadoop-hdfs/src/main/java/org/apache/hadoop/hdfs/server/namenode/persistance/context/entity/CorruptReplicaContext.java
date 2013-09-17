package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.CorruptReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LockUpgradeException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class CorruptReplicaContext extends EntityContext<CorruptReplica> {

//  protected Map<CorruptReplica, CorruptReplica> corruptReplicas = new HashMap<CorruptReplica, CorruptReplica>();
  protected Map<Long, Set<CorruptReplica>> blockCorruptReplicas = new HashMap<Long, Set<CorruptReplica>>();
  protected Map<CorruptReplica, CorruptReplica> newCorruptReplicas = new HashMap<CorruptReplica, CorruptReplica>();
  protected Map<CorruptReplica, CorruptReplica> removedCorruptReplicas = new HashMap<CorruptReplica, CorruptReplica>();
  protected boolean allCorruptBlocksRead = false;
  private CorruptReplicaDataAccess dataAccess;
//  private int nullCount = 0;

  public CorruptReplicaContext(CorruptReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(CorruptReplica entity) throws PersistanceException {
    if (removedCorruptReplicas.get(entity) != null) {
      throw new TransactionContextException("Removed corrupt replica passed to be persisted");
    }
//    if (corruptReplicas.containsKey(entity) && corruptReplicas.get(entity) == null) {
////      nullCount--;
//    }
//    corruptReplicas.put(entity, entity);
    newCorruptReplicas.put(entity, entity);
    Set<CorruptReplica> list = blockCorruptReplicas.get(entity.getBlockId());
    if(list == null)
    {
        list = new TreeSet<CorruptReplica>();
        blockCorruptReplicas.put(entity.getBlockId(), list);
    }
    list.add(entity);
    
    log("added-corrupt", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()), "sid", entity.getStorageId()});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
//    corruptReplicas.clear();
    blockCorruptReplicas.clear();
    newCorruptReplicas.clear();
    removedCorruptReplicas.clear();
    allCorruptBlocksRead = false;
//    nullCount = 0;
  }

  @Override
  public int count(CounterType<CorruptReplica> counter, Object... params) throws PersistanceException {
//    CorruptReplica.Counter cCounter = (CorruptReplica.Counter) counter;
//
//    switch (cCounter) {
//      case All:
//        if (allCorruptBlocksRead) {
//          log("count-all-corrupts", CacheHitState.HIT);
//          return corruptReplicas.size() - nullCount;
//        } else {
//          log("count-all-corrupts", CacheHitState.LOSS);
//          return dataAccess.countAll() + newCorruptReplicas.size() - removedCorruptReplicas.size();
//        }
//    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public CorruptReplica find(FinderType<CorruptReplica> finder, Object... params) throws PersistanceException {
//    CorruptReplica.Finder cFinder = (CorruptReplica.Finder) finder;
//
//    switch (cFinder) {
//      case ByPk:
//        Long blockId = (Long) params[0];
//        String storageId = (String) params[1];
//        CorruptReplica result = null;
//        CorruptReplica searchKey = new CorruptReplica(blockId, storageId);
//        // if corrupt replica was queried by bid before and it wasn't found.
//        if (blockCorruptReplicas.containsKey(blockId) && !blockCorruptReplicas.get(blockId).contains(searchKey))
//        {
//          log("find-corrupt-by-pk-not-exist", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
//          return null;
//        }
//        // otherwise it should exist or it's the first time that we query for it
//        if (corruptReplicas.containsKey(searchKey)) {
//          log("find-corrupt-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
//          result = corruptReplicas.get(searchKey);
//        } else {
//          log("find-corrupt-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "sid", storageId});
//          aboutToAccessStorage();
//          result = dataAccess.findByPk(blockId, storageId);
//          if (result == null) {
//            nullCount++;
//          }
//          corruptReplicas.put(searchKey, result);
//        }
//        return result;
//    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<CorruptReplica> findList(FinderType<CorruptReplica> finder, Object... params) throws PersistanceException {
    CorruptReplica.Finder cFinder = (CorruptReplica.Finder) finder;

    switch (cFinder) {
//      case All:
//        if (allCorruptBlocksRead) {
//          log("find-all-corrupts", CacheHitState.HIT);
//        } else {
//          log("find-all-corrupts", CacheHitState.LOSS);
//          aboutToAccessStorage();
//          syncCorruptReplicaInstances(dataAccess.findAll());
//          allCorruptBlocksRead = true;
//        }
//        List<CorruptReplica> list = new ArrayList<CorruptReplica>();
//        for (CorruptReplica cr : corruptReplicas.values()) {
//          if (cr != null) {
//            list.add(cr);
//          }
//        }
//        return list;
      case ByBlockId:
        Long blockId = (Long) params[0];
        if (blockCorruptReplicas.containsKey(blockId)) {
          log("find-corrupts-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId)});
          return new ArrayList(blockCorruptReplicas.get(blockId));
        }
        log("find-corrupts-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId)});
        aboutToAccessStorage();
        Set<CorruptReplica> list = new TreeSet(dataAccess.findByBlockId(blockId));
        blockCorruptReplicas.put(blockId, list);
        return new ArrayList(blockCorruptReplicas.get(blockId)); // Shallow copy

    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

    @Override
    public void prepare(TransactionLockManager tlm) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions

        if ((removedCorruptReplicas.values().size() != 0)
                && tlm.getCrLock() != TransactionLockManager.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade corrupt replica locks");
        }
        dataAccess.prepare(removedCorruptReplicas.values(), newCorruptReplicas.values(), null);
    }

  @Override
  public void remove(CorruptReplica entity) throws PersistanceException {
//    corruptReplicas.remove(entity);
    newCorruptReplicas.remove(entity);
    removedCorruptReplicas.put(entity, entity);
    if (blockCorruptReplicas.containsKey(entity.getBlockId())) {
      blockCorruptReplicas.get(entity.getBlockId()).remove(entity);
    }
    log("removed-corrupt", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()), "sid", entity.getStorageId()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(CorruptReplica entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param crs Returns only the fetched data from storage not those cached in
   * the memory.
   * @return
   */
//  private List<CorruptReplica> syncCorruptReplicaInstances(List<CorruptReplica> crs) {
//
//    ArrayList<CorruptReplica> finalList = new ArrayList<CorruptReplica>();
//
//    for (CorruptReplica replica : crs) {
//      if (removedCorruptReplicas.containsKey(replica)) {
//        continue;
//      }
//      if (corruptReplicas.containsKey(replica)) {
//        if (corruptReplicas.get(replica) == null) {
//          corruptReplicas.put(replica, replica);
//          nullCount--;
//        }
//        finalList.add(corruptReplicas.get(replica));
//      } else {
//        corruptReplicas.put(replica, replica);
//        finalList.add(replica);
//      }
//    }
//
//    return finalList;
//  }
}
