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
    log("added-leader", "id", hopLeader.getId(), "hostName",
        hopLeader.getHostName(), "counter", hopLeader.getCounter(),
        "timeStamp", hopLeader.getTimeStamp());
  }

  @Override
  public void remove(HopLeader hopLeader) throws TransactionContextException {
    super.remove(hopLeader);
    log("removed-leader", "id", hopLeader.getId(), "hostName",
        hopLeader.getHostName(), "counter", hopLeader.getCounter(),
        "timeStamp", hopLeader.getTimeStamp());
  }

  @Override
  public HopLeader find(FinderType<HopLeader> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopLeader.Finder lFinder = (HopLeader.Finder) finder;
    switch (lFinder) {
      case ById:
        return findById(lFinder, params);
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopLeader> findList(FinderType<HopLeader> finder,
      Object... params) throws TransactionContextException, StorageException {
    HopLeader.Finder lFinder = (HopLeader.Finder) finder;
    switch (lFinder) {
      case All:
        return findAll(lFinder);
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

  private HopLeader findById(HopLeader.Finder lFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long id = (Long) params[0];
    final int partitionKey = (Integer) params[1];
    HopLeader result = null;
    if (allRead || contains(id)) {
      result = get(id);
      hit(lFinder, result, "leaderId", id);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findByPkey(id, partitionKey);
      gotFromDB(id, result);
      miss(lFinder, result, "leaderId", id);
    }
    return result;
  }

  private Collection<HopLeader> findAll(HopLeader.Finder lFinder)
      throws StorageCallPreventedException, StorageException {
    Collection<HopLeader> result = null;
    if (allRead) {
      result = getAll();
      hit(lFinder, result);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findAll();
      allRead = true;
      gotFromDB(result);
      miss(lFinder, result);
    }
    return new ArrayList<HopLeader>(result);
  }

}
