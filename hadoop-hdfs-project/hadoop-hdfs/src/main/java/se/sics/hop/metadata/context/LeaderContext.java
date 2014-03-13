/*
 * Copyright 2012 Apache Software Foundation.
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
package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author salman
 */
public class LeaderContext extends EntityContext<HopLeader> {

  private Map<Long, HopLeader> allReadLeaders = new HashMap<Long, HopLeader>();
  private Map<Long, HopLeader> modifiedLeaders = new HashMap<Long, HopLeader>();
  private Map<Long, HopLeader> removedLeaders = new HashMap<Long, HopLeader>();
  private Map<Long, HopLeader> newLeaders = new HashMap<Long, HopLeader>();
  private boolean allRead = false;
  private LeaderDataAccess<HopLeader> dataAccess;

  public LeaderContext(LeaderDataAccess<HopLeader> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopLeader leader) throws PersistanceException {
    if (removedLeaders.containsKey(leader.getId())) {
      throw new TransactionContextException("Removed leader passed for persistance");
    }

    newLeaders.put(leader.getId(), leader);

    //put new leader in the AllReadLeaders List
    //for the future operatrions in the same transaction
    //
    allReadLeaders.put(leader.getId(), leader);

    log("added-leader", CacheHitState.NA,
            new String[]{
      "id", Long.toString(leader.getId()), "hostName", leader.getHostName(), "counter", Long.toString(leader.getCounter()),
      "timeStamp", Long.toString(leader.getTimeStamp())
    });
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    modifiedLeaders.clear();
    removedLeaders.clear();
    newLeaders.clear();
    allReadLeaders.clear();
    allRead = false;
  }

  @Override
  public int count(CounterType<HopLeader> counter, Object... params) throws PersistanceException {
    HopLeader.Counter lCounter = (HopLeader.Counter) counter;

    switch (lCounter) {
      case All:
        log("count-all-leaders", CacheHitState.LOSS);
        if (allRead) {
          return allReadLeaders.size();
        } else {
          aboutToAccessStorage();
          //allRead = true; //[S] commented this line. does not make sense
          return dataAccess.countAll();
        }
      case AllPredecessors:
        log("count-all-predecessor-leaders", CacheHitState.LOSS);
        Long id = (Long) params[0];
        if (allRead) {
          return findPredLeadersFromMapping(id).size();
        } else {
          aboutToAccessStorage();
          return dataAccess.countAllPredecessors(id);
        }
      case AllSuccessors:
        log("count-all-Successor-leaders", CacheHitState.LOSS);
        id = (Long) params[0];
        if (allRead) {
          return findSuccLeadersFromMapping(id).size();
        } else {
          aboutToAccessStorage();
          return dataAccess.countAllSuccessors(id);
        }
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);

  }

  private List<HopLeader> findPredLeadersFromMapping(long id) {
    List<HopLeader> preds = new ArrayList<HopLeader>();
    for (HopLeader leader : allReadLeaders.values()) {
      if (leader.getId() < id) {
        preds.add(leader);
      }
    }

    return preds;
  }

  private List<HopLeader> findLeadersWithGreaterCounterFromMapping(long counter) {
    List<HopLeader> greaterLeaders = new ArrayList<HopLeader>();
    for (HopLeader leader : allReadLeaders.values()) {
      if (leader.getCounter() > counter) {
        greaterLeaders.add(leader);
      }
    }

    return greaterLeaders;
  }

  private List<HopLeader> findSuccLeadersFromMapping(long id) {
    List<HopLeader> preds = new ArrayList<HopLeader>();
    for (HopLeader leader : allReadLeaders.values()) {
      if (leader.getId() > id) {
        preds.add(leader);
      }
    }

    return preds;
  }

  @Override
  public HopLeader find(FinderType<HopLeader> finder, Object... params) throws PersistanceException {
    HopLeader.Finder lFinder = (HopLeader.Finder) finder;
    HopLeader leader;

    switch (lFinder) {
      case ById:
        Long id = (Long) params[0];
        int partitionKey = (Integer) params[1];
        if (allRead || allReadLeaders.containsKey(id)) {
          log("find-leader-by-id", CacheHitState.HIT, new String[]{"leaderId", Long.toString(id)});
          leader = allReadLeaders.get(id);
        } else {
          log("find-leader-by-id", CacheHitState.LOSS, new String[]{
            "leaderId", Long.toString(id)
          });
          aboutToAccessStorage();
          leader = dataAccess.findByPkey(id, partitionKey);
          allReadLeaders.put(id, leader);
        }
        return leader;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopLeader> findList(FinderType<HopLeader> finder, Object... params) throws PersistanceException {
    HopLeader.Finder lFinder = (HopLeader.Finder) finder;
    Collection<HopLeader> leaders = null;

    switch (lFinder) {
      case AllByCounterGTN:
        long counter = (Long) params[0];
        // we are looking for range of row
        // not a good idea for running a range query on the cache
        // just get the damn rows from ndb
        if (allRead) {
          leaders = findLeadersWithGreaterCounterFromMapping(counter);
        } else {
          aboutToAccessStorage();
          leaders = dataAccess.findAllByCounterGT(counter);

          // put all read leaders in the cache
          if (leaders != null) {
            for (HopLeader leader : leaders) {
              if (!allReadLeaders.containsKey(leader.getId())) {
                allReadLeaders.put(leader.getId(), leader);
              }
            }
          }
        }

        return leaders;

      case AllByIDLT:
        long id = (Long) params[0];
        if (allRead) {
          leaders = findPredLeadersFromMapping(id);
        } else {
          aboutToAccessStorage();
          leaders = dataAccess.findAllByIDLT(id);

          // put all read leaders in the cache
          if (leaders != null) {
            for (HopLeader leader : leaders) {
              if (!allReadLeaders.containsKey(leader.getId())) {
                allReadLeaders.put(leader.getId(), leader);
              }
            }
          }
        }
        return leaders;

      case All:
        if (allRead) {
          leaders = allReadLeaders.values();
        } else {
          aboutToAccessStorage();
          leaders = dataAccess.findAll();
          allRead = true;
          // put all read leaders in the cache
          if (leaders != null) {
            for (HopLeader leader : leaders) {
              if (!allReadLeaders.containsKey(leader.getId())) {
                allReadLeaders.put(leader.getId(), leader);
              }
            }
          }
        }
        return new ArrayList(leaders);
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
    if ((!removedLeaders.values().isEmpty()
            || !modifiedLeaders.values().isEmpty())
            && hlks.getLeaderLock() != TransactionLockTypes.LockType.WRITE) {
      throw new LockUpgradeException("Trying to upgrade leader locks");
    }
    dataAccess.prepare(removedLeaders.values(), newLeaders.values(), modifiedLeaders.values());
  }

  @Override
  public void remove(HopLeader leader) throws PersistanceException {
    removedLeaders.put(leader.getId(), leader);

    if (allReadLeaders.containsKey(leader.getId())) {
      allReadLeaders.remove(leader.getId());
    }

    log("removed-leader", CacheHitState.NA, new String[]{
      "id", Long.toString(leader.getId()), "hostName", leader.getHostName(), "counter", Long.toString(leader.getCounter()),
      "timeStamp", Long.toString(leader.getTimeStamp())
    });
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void update(HopLeader leader) throws PersistanceException {
    if (removedLeaders.containsKey(leader.getId())) {
      throw new TransactionContextException("Trying to update a removed leader record");
    }

    modifiedLeaders.put(leader.getId(), leader);

    // update the allReadLeaders cache
    if (allReadLeaders.containsKey(leader.getId())) {
      allReadLeaders.remove(leader.getId());
    }
    allReadLeaders.put(leader.getId(), leader);

    log("updated-leader", CacheHitState.NA, new String[]{
      "id", Long.toString(leader.getId()), "hostName", leader.getHostName(), "counter", Long.toString(leader.getCounter()),
      "timeStamp", Long.toString(leader.getTimeStamp())
    });
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Leader",newLeaders.size(),modifiedLeaders.size(),removedLeaders.size());
    return stat;
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params) throws PersistanceException {
    
  }
}
