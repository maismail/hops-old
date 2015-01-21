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
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class InvalidatedBlockContext extends
    BaseReplicaContext<BlockPK.ReplicaPK
        , HopInvalidatedBlock> {

  private final InvalidateBlockDataAccess<HopInvalidatedBlock> dataAccess;
  private boolean allInvBlocksRead = false;

  public InvalidatedBlockContext(
      InvalidateBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(HopInvalidatedBlock hopInvalidatedBlock)
      throws TransactionContextException {
    super.update(hopInvalidatedBlock);
    log("added-invblock", "bid", hopInvalidatedBlock.getBlockId(),
        "sid", hopInvalidatedBlock.getStorageId());
  }

  @Override
  public void remove(HopInvalidatedBlock hopInvalidatedBlock)
      throws TransactionContextException {
    super.remove(hopInvalidatedBlock);
    log("removed-invblock", "bid", hopInvalidatedBlock.getBlockId(),
        "sid", hopInvalidatedBlock.getStorageId());
  }

  @Override
  public HopInvalidatedBlock find(FinderType<HopInvalidatedBlock> finder,
      Object... params) throws TransactionContextException, StorageException {
    HopInvalidatedBlock.Finder iFinder = (HopInvalidatedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdStorageIdAndINodeId:
        return findByPrimaryKey(iFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopInvalidatedBlock> findList(
      FinderType<HopInvalidatedBlock> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopInvalidatedBlock.Finder iFinder = (HopInvalidatedBlock.Finder) finder;
    switch (iFinder) {
      case ByBlockIdAndINodeId:
        return findByBlockId(iFinder, params);
      case ByINodeId:
        return findByINodeId(iFinder, params);
      case All:
        return findAll(iFinder);
      case ByBlockIdsStorageIdsAndINodeIds:
        return findByPrimaryKeys(iFinder, params);
      case ByINodeIds:
        return findByINodeIds(iFinder, params);
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
    allInvBlocksRead = false;
  }

  @Override
  HopInvalidatedBlock cloneEntity(HopInvalidatedBlock hopInvalidatedBlock) {
    return cloneEntity(hopInvalidatedBlock, hopInvalidatedBlock.getInodeId());
  }

  @Override
  HopInvalidatedBlock cloneEntity(HopInvalidatedBlock hopInvalidatedBlock,
      int inodeId) {
    return new HopInvalidatedBlock(hopInvalidatedBlock.getStorageId(),
        hopInvalidatedBlock.getBlockId(), inodeId);
  }

  @Override
  BlockPK.ReplicaPK getKey(HopInvalidatedBlock hopInvalidatedBlock) {
    return new BlockPK.ReplicaPK(hopInvalidatedBlock.getBlockId(),
        hopInvalidatedBlock.getInodeId(), hopInvalidatedBlock
        .getStorageId());
  }

  @Override
  protected boolean snapshotChanged() {
    return !getRemoved().isEmpty();
  }

  private HopInvalidatedBlock findByPrimaryKey(HopInvalidatedBlock.Finder
      iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int storageId = (Integer) params[1];
    final int inodeId = (Integer) params[2];
    final BlockPK.ReplicaPK key = new BlockPK.ReplicaPK(blockId, inodeId,
        storageId);
    HopInvalidatedBlock result = null;
    if (contains(key) || containsByBlock(blockId) || containsByINode(inodeId)) {
      result = get(key);
      hit(iFinder, result, "bid", blockId, "sid", storageId,
          "inodeId", inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findInvBlockByPkey(blockId, storageId, inodeId);
      gotFromDB(key, result);
      miss(iFinder, result, "bid", blockId, "sid", storageId,
          "inodeId", inodeId);
    }
    return result;
  }

  private List<HopInvalidatedBlock> findByBlockId(HopInvalidatedBlock.Finder
      iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    List<HopInvalidatedBlock> result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      result = getByBlock(blockId);
      hit(iFinder, result, "bid", blockId, "inodeId", inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findInvalidatedBlocksByBlockId(blockId, inodeId);
      Collections.sort(result);
      gotFromDB(new BlockPK(blockId), result);
      miss(iFinder, result, "bid", blockId, "inodeId", inodeId);
    }
    return result;
  }

  private List<HopInvalidatedBlock> findByINodeId(HopInvalidatedBlock.Finder
      iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<HopInvalidatedBlock> result = null;
    if (containsByINode(inodeId)) {
      result = getByINode(inodeId);
      hit(iFinder, result, "inodeId", inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findInvalidatedBlocksByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
      miss(iFinder, result, "inodeId", inodeId);
    }
    return result;
  }

  private List<HopInvalidatedBlock> findAll(HopInvalidatedBlock.Finder
      iFinder)
      throws StorageCallPreventedException, StorageException {
    List<HopInvalidatedBlock> result = null;
    if (allInvBlocksRead) {
      result = new ArrayList<HopInvalidatedBlock>(getAll());
      hit(iFinder, result);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findAllInvalidatedBlocks();
      gotFromDB(result);
      allInvBlocksRead = true;
      miss(iFinder, result);
    }
    return result;
  }

  private List<HopInvalidatedBlock> findByPrimaryKeys(HopInvalidatedBlock.Finder
      iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long[] blockIds = (long[]) params[0];
    final int[] inodeIds = (int[]) params[1];
    final int sid = (Integer) params[2];
    final int[] sids = new int[blockIds.length];
    Arrays.fill(sids, sid);
    aboutToAccessStorage();
    List<HopInvalidatedBlock> result = dataAccess.findInvalidatedBlocksbyPKS(
        blockIds, inodeIds, sids);
    gotFromDB(BlockPK.ReplicaPK.getKeys(blockIds, inodeIds, sid), result);
    miss(iFinder, result, "bids", Arrays.toString(blockIds), "inodeIds", Arrays
        .toString(inodeIds), "sid", sid);
    return result;
  }

  private List<HopInvalidatedBlock> findByINodeIds(HopInvalidatedBlock.Finder
      iFinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    aboutToAccessStorage();
    List<HopInvalidatedBlock> result = dataAccess.findInvalidatedBlocksByINodeIds(inodeIds);
    gotFromDB(BlockPK.ReplicaPK.getKeys(inodeIds), result);
    miss(iFinder, result, "inodeIds", Arrays.toString(inodeIds));
    return result;
  }
}
