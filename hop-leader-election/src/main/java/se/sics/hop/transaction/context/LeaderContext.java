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

import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;

public class LeaderContext extends BaseEntityContext<Long, HopLeader> {

  private final LeaderDataAccess<HopLeader> dataAccess;
  private boolean allRead = false;

  public LeaderContext(LeaderDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(HopLeader hopLeader) throws TransactionContextException {
    super.update(hopLeader);
    log("added-leader", CacheHitState.NA,
        new String[]{
            "id", Long.toString(hopLeader.getId()), "hostName",
            hopLeader.getHostName(), "counter",
            Long.toString(hopLeader.getCounter()),
            "timeStamp", Long.toString(hopLeader.getTimeStamp())
        });
  }

  @Override
  public void remove(HopLeader hopLeader) throws TransactionContextException {
    super.remove(hopLeader);
    log("removed-leader", CacheHitState.NA, new String[]{
        "id", Long.toString(hopLeader.getId()), "hostName",
        hopLeader.getHostName(), "counter",
        Long.toString(hopLeader.getCounter()),
        "timeStamp", Long.toString(hopLeader.getTimeStamp())
    });
  }

  @Override
  public HopLeader find(FinderType<HopLeader> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopLeader.Finder lFinder = (HopLeader.Finder) finder;
    switch (lFinder) {
      case ById:
        return findById(params);
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopLeader> findList(FinderType<HopLeader> finder,
      Object... params) throws TransactionContextException, StorageException {
    HopLeader.Finder lFinder = (HopLeader.Finder) finder;
    switch (lFinder) {
      case All:
        return findAll();
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    allRead = false;
  }

  @Override
  Long getKey(HopLeader hopLeader) {
    return hopLeader.getId();
  }

  private HopLeader findById(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long id = (Long) params[0];
    final int partitionKey = (Integer) params[1];
    HopLeader result = null;
    if (allRead || contains(id)) {
      log("find-leader-by-id", CacheHitState.HIT,
          new String[]{"leaderId", Long.toString(id)});
      result = get(id);
    } else {
      log("find-leader-by-id", CacheHitState.LOSS, new String[]{
          "leaderId", Long.toString(id)
      });
      aboutToAccessStorage();
      result = dataAccess.findByPkey(id, partitionKey);
      gotFromDB(id, result);
    }
    return result;
  }

  private Collection<HopLeader> findAll()
      throws StorageCallPreventedException, StorageException {
    Collection<HopLeader> result = null;
    if (allRead) {
      result = getAll();
    } else {
      aboutToAccessStorage();
      result = dataAccess.findAll();
      allRead = true;
      gotFromDB(result);
    }
    return new ArrayList<HopLeader>(result);
  }

}
