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
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CorruptReplicaContext extends
    BaseReplicaContext<BlockPK.ReplicaPK
        , HopCorruptReplica> {

  CorruptReplicaDataAccess dataAccess;

  public CorruptReplicaContext(
      CorruptReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(HopCorruptReplica hopCorruptReplica)
      throws TransactionContextException {
    super.update(hopCorruptReplica);
    log("added-corrupt", "bid", hopCorruptReplica.getBlockId(),
        "sid", hopCorruptReplica.getStorageId());
  }

  @Override
  public void remove(HopCorruptReplica hopCorruptReplica)
      throws TransactionContextException {
    super.remove(hopCorruptReplica);
    log("removed-corrupt", "bid", hopCorruptReplica.getBlockId(),
        "sid", hopCorruptReplica.getStorageId());
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  public Collection<HopCorruptReplica> findList(
      FinderType<HopCorruptReplica> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopCorruptReplica.Finder cFinder = (HopCorruptReplica.Finder) finder;
    switch (cFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(cFinder, params);
      case ByINodeId:
        return findByINodeId(cFinder, params);
      case ByINodeIds:
        return findByINodeIds(cFinder, params);
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  HopCorruptReplica cloneEntity(HopCorruptReplica hopCorruptReplica) {
    return cloneEntity(hopCorruptReplica, hopCorruptReplica.getInodeId());
  }

  @Override
  HopCorruptReplica cloneEntity(HopCorruptReplica hopCorruptReplica,
      int inodeId) {
    return new HopCorruptReplica(hopCorruptReplica.getBlockId(),
        hopCorruptReplica.getStorageId(), inodeId);
  }

  @Override
  BlockPK.ReplicaPK getKey(HopCorruptReplica hopCorruptReplica) {
    return new BlockPK.ReplicaPK(hopCorruptReplica.getBlockId(),
        hopCorruptReplica.getInodeId(), hopCorruptReplica
        .getStorageId());
  }

  private List<HopCorruptReplica> findByBlockId(HopCorruptReplica.Finder
      cFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<HopCorruptReplica> result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(cFinder, result, "bid", blockId,"inodeid",inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findByBlockId(blockId, inodeId);
      Collections.sort(result);
      gotFromDB(new BlockPK(blockId), result);
      miss(cFinder, result, "bid", blockId, "inodeid", inodeId);
    }
    return result;
  }

  private List<HopCorruptReplica> findByINodeId(HopCorruptReplica.Finder
      cFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<HopCorruptReplica> result = null;
    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(cFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
      miss(cFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<HopCorruptReplica> findByINodeIds(HopCorruptReplica.Finder
      cFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    aboutToAccessStorage();
    List<HopCorruptReplica> result = dataAccess.findByINodeIds(inodeIds);
    miss(cFinder, result, "inodeids", Arrays.toString(inodeIds));
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    return result;
  }

}
