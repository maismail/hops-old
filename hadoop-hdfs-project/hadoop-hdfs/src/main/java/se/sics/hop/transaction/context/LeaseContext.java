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

import org.apache.hadoop.hdfs.server.namenode.Lease;
import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.HashMap;
import java.util.Map;

public class LeaseContext extends BaseEntityContext<String, Lease> {

  private final LeaseDataAccess<Lease> dataAccess;
  private final Map<Integer, Lease> idToLease = new HashMap<Integer, Lease>();

  public LeaseContext(
      LeaseDataAccess<Lease> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(Lease lease) throws TransactionContextException {
    super.update(lease);
    idToLease.put(lease.getHolderID(), lease);
    log("added-lease", CacheHitState.NA,
        new String[]{"holder", lease.getHolder(),
            "hid", String.valueOf(lease.getHolderID())});
  }

  @Override
  public int count(CounterType<Lease> counter, Object... params)
      throws TransactionContextException, StorageException {
    Lease.Counter lCounter = (Lease.Counter) counter;
    switch (lCounter) {
      case All:
        log("count-all-leases", CacheHitState.LOSS);
        return dataAccess.countAll();
    }
    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public Lease find(FinderType<Lease> finder, Object... params)
      throws TransactionContextException, StorageException {
    Lease.Finder lFinder = (Lease.Finder) finder;
    switch (lFinder) {
      case ByPKey:
        return findByHolder(params);
      case ByHolderId:
        return findByHolderId(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void remove(Lease lease) throws TransactionContextException {
    super.remove(lease);
    idToLease.remove(lease.getHolderID());
    log("removed-lease", CacheHitState.NA,
        new String[]{"holder", lease.getHolder()});
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    idToLease.clear();
  }

  @Override
  String getKey(Lease lease) {
    return lease.getHolder();
  }

  private Lease findByHolder(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final String holder = (String) params[0];
    Lease result = null;
    if (contains(holder)) {
      log("find-lease-by-pk", CacheHitState.HIT,
          new String[]{"holder", holder});
      result = get(holder);
    } else {
      log("find-lease-by-pk", CacheHitState.LOSS,
          new String[]{"holder", holder});
      aboutToAccessStorage();
      result = dataAccess.findByPKey(holder);
      gotFromDB(holder, result);
      if (result != null) {
        idToLease.put(result.getHolderID(), result);
      }
    }
    return result;
  }

  private Lease findByHolderId(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int holderId = (Integer) params[0];
    Lease result = null;
    if (idToLease.containsKey(holderId)) {
      log("find-lease-by-holderid", CacheHitState.HIT,
          new String[]{"hid", Integer.toString(holderId)});
      result = idToLease.get(holderId);
    } else {
      log("find-lease-by-holderid", CacheHitState.LOSS,
          new String[]{"hid", Integer.toString(holderId)});
      aboutToAccessStorage();
      result = dataAccess.findByHolderId(holderId);
      gotFromDB(result);
      idToLease.put(holderId, result);
    }
    return result;
  }

}
