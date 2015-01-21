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
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ReplicaContext extends BaseReplicaContext<BlockPK.ReplicaPK
    , HopIndexedReplica> {

  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(HopIndexedReplica replica)
      throws TransactionContextException {
    super.update(replica);
    log("updated-replica", "bid", replica.getBlockId(),
        "sid", replica.getStorageId(), "index", replica.getIndex());
  }

  @Override
  public void remove(HopIndexedReplica replica)
      throws TransactionContextException {
    super.remove(replica);
    log("removed-replica", "bid", replica.getBlockId(),
        "sid", replica.getStorageId(), "index", replica.getIndex());
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<HopIndexedReplica> findList(
      FinderType<HopIndexedReplica> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopIndexedReplica.Finder iFinder = (HopIndexedReplica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(iFinder, params);
      case ByINodeId:
        return findByINodeId(iFinder, params);
      case ByBlockIdsStorageIdsAndINodeIds:
        return findByPrimaryKeys(iFinder, params);
      case ByINodeIds:
        return findyByINodeIds(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public HopIndexedReplica find(FinderType<HopIndexedReplica> finder,
      Object... params) throws TransactionContextException, StorageException {
    HopIndexedReplica.Finder iFinder = (HopIndexedReplica.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndStorageId:
        return findByPK(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  BlockPK.ReplicaPK getKey(HopIndexedReplica hopIndexedReplica) {
    return new BlockPK.ReplicaPK(hopIndexedReplica.getBlockId(),
        hopIndexedReplica.getInodeId(), hopIndexedReplica.getStorageId());
  }

  @Override
  HopIndexedReplica cloneEntity(HopIndexedReplica hopIndexedReplica) {
    return cloneEntity(hopIndexedReplica, hopIndexedReplica.getInodeId());
  }

  @Override
  HopIndexedReplica cloneEntity(HopIndexedReplica hopIndexedReplica,
      int inodeId) {
    return new HopIndexedReplica(hopIndexedReplica.getBlockId(),
        hopIndexedReplica.getStorageId(), inodeId,
        hopIndexedReplica.getIndex());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getAdded().isEmpty() || !getModified().isEmpty();
  }

  private HopIndexedReplica findByPK(HopIndexedReplica.Finder iFinder,
      Object[] params) {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    HopIndexedReplica result = null;
    List<HopIndexedReplica> replicas = getByBlock(blockId);
    if (replicas != null) {
      for (HopIndexedReplica replica : replicas) {
        if (replica != null) {
          if (replica.getStorageId() == storageId) {
              result = replica;
            break;
          }
        }
      }
    }
    hit(iFinder, result, "bid", blockId, "sid", storageId);
    return result;
  }

  private List<HopIndexedReplica> findByBlockId(HopIndexedReplica.Finder
      iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<HopIndexedReplica> results = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      results = getByBlock(blockId);
      hit(iFinder, results, "bid", blockId);
    } else {
      aboutToAccessStorage();
      results = dataAccess.findReplicasById(blockId, inodeId);
      gotFromDB(new BlockPK(blockId), results);
      miss(iFinder, results, "bid", blockId);
    }
    return results;
  }

  private List<HopIndexedReplica> findByINodeId(HopIndexedReplica.Finder
      iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<HopIndexedReplica> results = null;
    if (containsByINode(inodeId)) {
      results = getByINode(inodeId);
      hit(iFinder, results, "inodeid", inodeId);
    } else {
      aboutToAccessStorage();
      results = dataAccess.findReplicasByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), results);
      miss(iFinder, results, "inodeid", inodeId);
    }
    return results;
  }

  private List<HopIndexedReplica> findByPrimaryKeys(HopIndexedReplica.Finder
      iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    long[] blockIds = (long[]) params[0];
    int[] inodeIds = (int[]) params[1];
    int sid = (Integer) params[2];
    int[] sids = new int[blockIds.length];
    Arrays.fill(sids, sid);
    List<HopIndexedReplica> results = dataAccess.findReplicasByPKS(blockIds,
        inodeIds, sids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(blockIds, sid), results);
    miss(iFinder, results, "blockIds", Arrays.toString(blockIds), "inodeIds",
        Arrays.toString(inodeIds), "sid", sid);
    return results;
  }

  private List<HopIndexedReplica> findyByINodeIds(HopIndexedReplica.Finder
      iFinder, Object[] params) throws
      StorageCallPreventedException, StorageException {
    int[] ids = (int[]) params[0];
    aboutToAccessStorage();
    List<HopIndexedReplica> results = dataAccess.findReplicasByINodeIds(ids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(ids), results);
    miss(iFinder, results, "inodeIds", Arrays.toString(ids));
    return results;
  }
}
