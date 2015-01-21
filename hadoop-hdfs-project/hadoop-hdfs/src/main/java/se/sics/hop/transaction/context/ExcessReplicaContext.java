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
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ExcessReplicaContext extends
    BaseReplicaContext<BlockPK.ReplicaPK
        , HopExcessReplica> {

  ExcessReplicaDataAccess<HopExcessReplica> dataAccess;

  public ExcessReplicaContext(
      ExcessReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(HopExcessReplica hopExcessReplica)
      throws TransactionContextException {
    super.update(hopExcessReplica);
    log("added-excess", "bid", hopExcessReplica.getBlockId(), "sid",
        hopExcessReplica.getStorageId());
  }

  @Override
  public void remove(HopExcessReplica hopExcessReplica)
      throws TransactionContextException {
    super.remove(hopExcessReplica);
    log("removed-excess", "bid", hopExcessReplica.getBlockId(), "sid",
        hopExcessReplica.getStorageId());
  }

  @Override
  public HopExcessReplica find(FinderType<HopExcessReplica> finder,
      Object... params) throws TransactionContextException, StorageException {
    HopExcessReplica.Finder eFinder = (HopExcessReplica.Finder) finder;
    switch (eFinder) {
      case ByBlockIdStorageIdAndINodeId:
        return findByPrimaryKey(eFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopExcessReplica> findList(
      FinderType<HopExcessReplica> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopExcessReplica.Finder eFinder = (HopExcessReplica.Finder) finder;
    switch (eFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(eFinder, params);
      case ByINodeId:
        return findByINodeId(eFinder, params);
      case ByINodeIds:
        return findByINodeIds(eFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  HopExcessReplica cloneEntity(HopExcessReplica hopExcessReplica) {
    return cloneEntity(hopExcessReplica, hopExcessReplica.getInodeId());
  }

  @Override
  HopExcessReplica cloneEntity(HopExcessReplica hopExcessReplica, int inodeId) {
    return new HopExcessReplica(hopExcessReplica.getStorageId(),
        hopExcessReplica.getBlockId(), inodeId);
  }

  @Override
  BlockPK.ReplicaPK getKey(HopExcessReplica hopExcessReplica) {
    return new BlockPK.ReplicaPK(hopExcessReplica.getBlockId(),
        hopExcessReplica.getInodeId(), hopExcessReplica
        .getStorageId());
  }

  private HopExcessReplica findByPrimaryKey(HopExcessReplica.Finder eFinder,
      Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    final int inodeId = (Integer) params[2];
    final BlockPK.ReplicaPK key = new BlockPK.ReplicaPK(blockId, inodeId,
        storageId);
    HopExcessReplica result = null;
    if (contains(key) || containsByINode(inodeId) || containsByBlock(blockId)) {
      result = get(key);
      hit(eFinder, result, "bid", blockId, "sid", storageId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findByPK(blockId, storageId, inodeId);
      gotFromDB(key, result);
      miss(eFinder, result, "bid", blockId, "sid", storageId);
    }
    return result;
  }

  private List<HopExcessReplica> findByBlockId(HopExcessReplica.Finder
      eFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<HopExcessReplica> result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(eFinder, result, "bid", blockId, "inodeId", inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findExcessReplicaByBlockId(blockId, inodeId);
      Collections.sort(result);
      gotFromDB(new BlockPK(blockId), result);
      miss(eFinder, result, "bid", blockId, "inodeId", inodeId);
    }
    return result;
  }

  private List<HopExcessReplica> findByINodeId(HopExcessReplica.Finder
      eFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<HopExcessReplica> result = null;
    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(eFinder, result, "inodeId", inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findExcessReplicaByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
      miss(eFinder, result, "inodeId", inodeId);
    }
    return result;
  }

  private List<HopExcessReplica> findByINodeIds(HopExcessReplica.Finder
      eFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    aboutToAccessStorage();
    List<HopExcessReplica> result = dataAccess.findExcessReplicaByINodeIds
        (inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    miss(eFinder, result, "inodeIds", Arrays.toString(inodeIds));
    return result;
  }


}
