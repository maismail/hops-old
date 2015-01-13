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
    log("updated-replica", CacheHitState.NA,
        new String[]{"bid", Long.toString(replica.getBlockId()),
            "sid", Integer.toString(replica.getStorageId()), "index",
            Integer.toString(replica.getIndex())});
  }

  @Override
  public void remove(HopIndexedReplica replica)
      throws TransactionContextException {
    super.remove(replica);
    log("removed-replica", CacheHitState.NA,
        new String[]{"bid", Long.toString(replica.getBlockId()),
            "sid", Integer.toString(replica.getStorageId()), "index",
            Integer.toString(replica.getIndex())});
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
      case ByBlockId:
        return findByBlockId(params);
      case ByINodeId:
        return findByINodeId(params);
      case ByStorageId:
        return findByStorageId(params);
      case ByPKS:
        return findByPrimaryKeys(params);
      case ByINodeIds:
        return findyByINodeIds(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public HopIndexedReplica find(FinderType<HopIndexedReplica> finder,
      Object... params) throws TransactionContextException, StorageException {
    HopIndexedReplica.Finder iFinder = (HopIndexedReplica.Finder) finder;
    switch (iFinder) {
      case ByPK:
        return findByPK(params);
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
        hopIndexedReplica.getStorageId(), inodeId, hopIndexedReplica.getIndex());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getAdded().isEmpty() || !getModified().isEmpty();
  }

  private HopIndexedReplica findByPK(Object[] params) {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    List<HopIndexedReplica> replicas = getByBlock(blockId);
    if (replicas != null) {
      for (HopIndexedReplica replica : replicas) {
        if(replica != null) {
          if (replica.getStorageId() == storageId) {
            return replica;
          }
        }
      }
    }
    return null;
  }

  private List<HopIndexedReplica> findByBlockId(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<HopIndexedReplica> results = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      log("find-replicas-by-bid", CacheHitState.HIT,
          new String[]{"bid", Long.toString(blockId)});
      results = getByBlock(blockId);
    } else {
      log("find-replicas-by-bid", CacheHitState.LOSS,
          new String[]{"bid", Long.toString(blockId)});
      aboutToAccessStorage();
      results = dataAccess.findReplicasById(blockId, inodeId);
      gotFromDB(new BlockPK(blockId), results);
    }
    return results;
  }

  private List<HopIndexedReplica> findByINodeId(Object[] params) throws
      StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<HopIndexedReplica> results = null;
    if (containsByINode(inodeId)) {
      log("find-replicas-by-inode-id", CacheHitState.HIT,
          new String[]{"inode_id", Integer.toString(inodeId)});
      results = getByINode(inodeId);
    } else {
      log("find-replicas-by-inode-id", CacheHitState.LOSS,
          new String[]{"inode_id", Integer.toString(inodeId)});
      aboutToAccessStorage();
      results = dataAccess.findReplicasByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), results);
    }
    return results;
  }

  private List<HopIndexedReplica> findByStorageId(Object[] params) throws
      StorageCallPreventedException, StorageException {
    final long[] blockIds = (long[]) params[0];
    final int sid = (Integer) params[1];
    final int[] sids = new int[blockIds.length];
    Arrays.fill(sids, sid);
    log("find-replicas-by-sid", CacheHitState.NA,
        new String[]{"Ids", "" + blockIds, "sid", Integer.toString(sid)});
    List<HopIndexedReplica> results = dataAccess.findReplicasByStorageId(sid);
    gotFromDB(BlockPK.ReplicaPK.getKeys(blockIds, sid), results);
    return results;
  }

  private List<HopIndexedReplica> findByPrimaryKeys(Object[] params) throws
      StorageCallPreventedException, StorageException {
    long[] blockIds = (long[]) params[0];
    int[] inodeIds = (int[]) params[1];
    int sid = (Integer) params[2];
    int[] sids = new int[blockIds.length];
    Arrays.fill(sids, sid);
    log("find-replicas-by-pks", CacheHitState.NA,
        new String[]{"Ids", "" + blockIds, "InodeIds", "" + inodeIds, " sid",
            Integer.toString(sid)});
    List<HopIndexedReplica> results = dataAccess.findReplicasByPKS(blockIds,
        inodeIds, sids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(blockIds, sid), results);
    return results;
  }

  private List<HopIndexedReplica> findyByINodeIds(Object[] params) throws
      StorageCallPreventedException, StorageException {
    int[] ids = (int[]) params[0];
    log("find-replicas-by-inode-ids", CacheHitState.LOSS,
        new String[]{"inode_ids", Arrays.toString(ids)});
    aboutToAccessStorage();
    List<HopIndexedReplica> results = dataAccess.findReplicasByINodeIds(ids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(ids), results);
    return results;
  }
}
