/*
 * Copyright 2014 Apache Software Foundation.
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
package se.sics.hop.metadata;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.hdfs.dal.StorageIdMapDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopStorageId;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class StorageIdMap {

  private Map<String, Integer> storageIdtoSId;
  private Map<Integer, String> sIdtoStorageId;
  
  public StorageIdMap() throws IOException {
    this.sIdtoStorageId = new HashMap<Integer, String>();
    this.storageIdtoSId = new HashMap<String, Integer>();
    initialize();
  }

  public void update(DatanodeDescriptor dn) throws IOException {
    String storageId = dn.getStorageID();
    if (!storageIdtoSId.containsKey(storageId)) {
      getSetSId(storageId);
    }
    dn.setSId(storageIdtoSId.get(storageId));
  }

  public int getSId(String storageId) {
    return storageIdtoSId.get(storageId);
  }

  public String getStorageId(int sid) {
    return sIdtoStorageId.get(sid);
  }

  private void initialize() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.INITIALIZE_SID_MAP) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        StorageIdMapDataAccess<HopStorageId> da = (StorageIdMapDataAccess) StorageFactory.getDataAccess(StorageIdMapDataAccess.class);
        Collection<HopStorageId> sids = da.findAll();
        for (HopStorageId h : sids) {
          storageIdtoSId.put(h.getStorageId(), h.getsId());
          sIdtoStorageId.put(h.getsId(), h.getStorageId());
        }
        return null;
      }
    }.handle();
  }

  private void getSetSId(final String storageId) throws IOException {
    new HDFSTransactionalRequestHandler(HDFSOperationType.GET_SET_SID) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
        tla.getLocks().addSIdCounter(LockType.WRITE);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        int currSIdCount = Variables.getSIdCounter();
        StorageIdMapDataAccess<HopStorageId> da = (StorageIdMapDataAccess) StorageFactory.getDataAccess(StorageIdMapDataAccess.class);
        HopStorageId h = da.findByPk(storageId);
        if (h == null) {
          h = new HopStorageId(storageId, currSIdCount);
          da.add(h);
          currSIdCount++;
          Variables.setSIdCounter(currSIdCount);
        }

        storageIdtoSId.put(storageId, h.getsId());
        sIdtoStorageId.put(h.getsId(), storageId);

        return null;
      }
    }.handle();
  }
}
