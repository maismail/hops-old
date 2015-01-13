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

import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ReplicaUnderConstructionContext extends
    BaseReplicaContext<BlockPK.ReplicaPK
        , ReplicaUnderConstruction> {

  ReplicaUnderConstructionDataAccess dataAccess;

  public ReplicaUnderConstructionContext(
      ReplicaUnderConstructionDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(ReplicaUnderConstruction replica)
      throws TransactionContextException {
    super.update(replica);
    log("added-replicauc", CacheHitState.NA,
        new String[]{"bid", Long.toString(replica.getBlockId()),
            "sid", Integer.toString(replica.getStorageId()), "state",
            replica.getState().name()});
  }

  @Override
  public void remove(ReplicaUnderConstruction replica)
      throws TransactionContextException {
    super.remove(replica);
    log("removed-replicauc", CacheHitState.NA,
        new String[]{"bid", Long.toString(replica.getBlockId()),
            "sid", Integer.toString(replica.getStorageId()), "state",
            replica.getState().name()});
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<ReplicaUnderConstruction> findList(
      FinderType<ReplicaUnderConstruction> finder, Object... params)
      throws TransactionContextException, StorageException {
    ReplicaUnderConstruction.Finder rFinder =
        (ReplicaUnderConstruction.Finder) finder;
    switch (rFinder) {
      case ByBlockId:
        return findByBlockId(params);
      case ByINodeId:
        return findByINodeId(params);
      case ByINodeIds:
        return findByINodeIds(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  BlockPK.ReplicaPK getKey(ReplicaUnderConstruction replica) {
    return new BlockPK.ReplicaPK(replica.getBlockId(),
        replica.getInodeId(), replica
        .getStorageId());
  }

  @Override
  ReplicaUnderConstruction cloneEntity(
      ReplicaUnderConstruction replicaUnderConstruction) {
    return cloneEntity(replicaUnderConstruction, replicaUnderConstruction
        .getInodeId());
  }

  @Override
  ReplicaUnderConstruction cloneEntity(
      ReplicaUnderConstruction replicaUnderConstruction, int inodeId) {
    return new ReplicaUnderConstruction(replicaUnderConstruction.getState(),
        replicaUnderConstruction.getStorageId(), replicaUnderConstruction
        .getBlockId(), inodeId, replicaUnderConstruction.getIndex());
  }

  private List<ReplicaUnderConstruction> findByBlockId(Object[] params)
      throws TransactionContextException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<ReplicaUnderConstruction> result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      log("find-replicaucs-by-bid", CacheHitState.HIT,
          new String[]{"bid", Long.toString(blockId)});
      result = getByBlock(blockId);
    } else {
      log("find-replicaucs-by-bid", CacheHitState.LOSS,
          new String[]{"bid", Long.toString(blockId)});
      aboutToAccessStorage();
      result =
          dataAccess.findReplicaUnderConstructionByBlockId(blockId, inodeId);
      gotFromDB(new BlockPK(blockId), result);
    }
    return result;
  }

  private List<ReplicaUnderConstruction> findByINodeId(Object[] params)
      throws TransactionContextException, StorageException {
    final int inodeId = (Integer) params[0];
    List<ReplicaUnderConstruction> result = null;
    if (containsByINode(inodeId)) {
      log("find-replicaucs-by-inode-id", CacheHitState.HIT,
          new String[]{"inode_id", Integer.toString(inodeId)});
      result = getByINode(inodeId);
    } else {
      log("find-replicaucs-by-inode-id", CacheHitState.LOSS,
          new String[]{"inode_id", Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.findReplicaUnderConstructionByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
    }
    return result;
  }

  private List<ReplicaUnderConstruction> findByINodeIds(Object[] params)
      throws TransactionContextException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    log("find-replicaucs-by-inode-ids", CacheHitState.LOSS,
        new String[]{"inode_ids", Arrays.toString(
            inodeIds)});
    aboutToAccessStorage();
    List<ReplicaUnderConstruction> result = dataAccess
        .findReplicaUnderConstructionByINodeIds(inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    return result;
  }

}
