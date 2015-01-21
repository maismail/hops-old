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

import com.google.common.base.Predicate;
import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LeasePathContext extends BaseEntityContext<String, HopLeasePath> {

  private final LeasePathDataAccess<HopLeasePath> dataAccess;
  private final Map<Integer, Set<HopLeasePath>> holderIdToLeasePath = new
      HashMap<Integer, Set<HopLeasePath>>();

  public LeasePathContext(
      LeasePathDataAccess<HopLeasePath> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(HopLeasePath hopLeasePath)
      throws TransactionContextException {
    super.update(hopLeasePath);
    addInternal(hopLeasePath);
    log("added-lpath", "path", hopLeasePath.getPath(), "hid",
        hopLeasePath.getHolderId());
  }

  @Override
  public void remove(HopLeasePath hopLeasePath)
      throws TransactionContextException {
    super.remove(hopLeasePath);
    removeInternal(hopLeasePath);
    log("removed-lpath", "path", hopLeasePath.getPath());
  }

  @Override
  public HopLeasePath find(FinderType<HopLeasePath> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopLeasePath.Finder lFinder = (HopLeasePath.Finder) finder;
    switch (lFinder) {
      case ByPath:
        return findByPrimaryKey(lFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopLeasePath> findList(FinderType<HopLeasePath> finder,
      Object... params) throws TransactionContextException, StorageException {
    HopLeasePath.Finder lFinder = (HopLeasePath.Finder) finder;
    switch (lFinder) {
      case ByHolderId:
        return findByHolderId(lFinder, params);
      case ByPrefix:
        return findByPrefix(lFinder, params);
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
    holderIdToLeasePath.clear();
  }

  @Override
  String getKey(HopLeasePath hopLeasePath) {
    return hopLeasePath.getPath();
  }

  private HopLeasePath findByPrimaryKey(HopLeasePath.Finder lFinder, Object[]
      params)
      throws StorageCallPreventedException, StorageException {
    final String path = (String) params[0];
    HopLeasePath result = null;
    if (contains(path)) {
      result = get(path);
      hit(lFinder, result, "path", path);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findByPKey(path);
      gotFromDB(path, result);
      miss(lFinder, result, "path", path);
    }
    return result;
  }

  private Collection<HopLeasePath> findByHolderId(HopLeasePath.Finder
      lFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int holderId = (Integer) params[0];
    Collection<HopLeasePath> result = null;
    if (holderIdToLeasePath.containsKey(holderId)) {
      result = new ArrayList<HopLeasePath>(holderIdToLeasePath.get(holderId));
      hit(lFinder, result, "hid", holderId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findByHolderId(holderId);
      gotFromDB(holderId, result);
      miss(lFinder, result, "hid", holderId);
    }
    return result;
  }

  private Collection<HopLeasePath> findByPrefix(HopLeasePath.Finder lFinder,
      Object[] params)
      throws StorageCallPreventedException, StorageException {
    final String prefix = (String) params[0];
    Collection<HopLeasePath> result = null;
    try {
      aboutToAccessStorage();
      result = dataAccess.findByPrefix(prefix);
      gotFromDB(result);
      miss(lFinder, result, "prefix", prefix, "numOfLps", result.size());
    } catch (StorageCallPreventedException ex) {
      // This is allowed in querying lease-path by prefix, this is needed in delete operation for example.
      result = getFilteredByPrefix(prefix);
      hit(lFinder, result, "prefix", prefix, "numOfLps", result.size());
    }
    return result;
  }

  private Collection<HopLeasePath> getFilteredByPrefix(final String prefix) {
    return get(new Predicate<ContextEntity>() {
      @Override
      public boolean apply(ContextEntity input) {
        if (input.getState() != State.REMOVED) {
          HopLeasePath leasePath = input.getEntity();
          if (leasePath != null) {
            return leasePath.getPath().contains(prefix);
          }
        }
        return false;
      }
    });
  }

  @Override
  void gotFromDB(String entityKey, HopLeasePath leasePath) {
    super.gotFromDB(entityKey, leasePath);
    addInternal(leasePath);
  }

  @Override
  void gotFromDB(Collection<HopLeasePath> entityList) {
    super.gotFromDB(entityList);
    addInternal(entityList);
  }

  private void gotFromDB(int holderId, Collection<HopLeasePath> leasePaths) {
    gotFromDB(leasePaths);
    if (leasePaths == null) {
      addInternal(holderId, null);
    }
  }

  private void addInternal(Collection<HopLeasePath> leasePaths) {
    if (leasePaths == null) {
      return;
    }
    for (HopLeasePath leasePath : leasePaths) {
      addInternal(leasePath);
    }
  }

  private void addInternal(HopLeasePath leasePath) {
    if (leasePath == null) {
      return;
    }
    addInternal(leasePath.getHolderId(), leasePath);
  }

  private void addInternal(int holderId, HopLeasePath leasePath) {
    Set<HopLeasePath> hopLeasePaths = holderIdToLeasePath.get(holderId);
    if (hopLeasePaths == null) {
      hopLeasePaths = new HashSet<HopLeasePath>();
      holderIdToLeasePath.put(holderId, hopLeasePaths);
    }
    hopLeasePaths.add(leasePath);
  }

  private void removeInternal(HopLeasePath
      hopLeasePath) {
    Set<HopLeasePath> hopLeasePaths =
        holderIdToLeasePath.get(hopLeasePath.getHolderId());
    if (hopLeasePaths != null) {
      hopLeasePaths.remove(hopLeasePath);
    }
  }
}
