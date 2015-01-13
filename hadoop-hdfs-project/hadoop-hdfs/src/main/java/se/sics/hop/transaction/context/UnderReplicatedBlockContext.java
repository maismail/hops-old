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
import se.sics.hop.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class UnderReplicatedBlockContext extends
    BaseReplicaContext<BlockPK
        , HopUnderReplicatedBlock> {

  private final UnderReplicatedBlockDataAccess<HopUnderReplicatedBlock>
      dataAccess;

  public UnderReplicatedBlockContext(
      UnderReplicatedBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(HopUnderReplicatedBlock hopUnderReplicatedBlock)
      throws TransactionContextException {
    super.update(hopUnderReplicatedBlock);
    log("added-urblock", CacheHitState.NA,
        new String[]{"bid", Long.toString(hopUnderReplicatedBlock.getBlockId()),
            "level", Integer.toString(hopUnderReplicatedBlock.getLevel()),
            "inodeId", Integer.toString(hopUnderReplicatedBlock.getInodeId())});
  }

  @Override
  public void remove(HopUnderReplicatedBlock hopUnderReplicatedBlock)
      throws TransactionContextException {
    super.remove(hopUnderReplicatedBlock);
    log("removed-urblock", CacheHitState.NA,
        new String[]{"bid", Long.toString(hopUnderReplicatedBlock.getBlockId()),
            "level", Integer.toString(hopUnderReplicatedBlock.getLevel())});
  }

  @Override
  public HopUnderReplicatedBlock find(
      FinderType<HopUnderReplicatedBlock> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopUnderReplicatedBlock.Finder urFinder =
        (HopUnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case ByBlockId:
        return findByBlockId(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopUnderReplicatedBlock> findList(
      FinderType<HopUnderReplicatedBlock> finder, Object... params)
      throws TransactionContextException, StorageException {
    HopUnderReplicatedBlock.Finder urFinder =
        (HopUnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case ByINodeId:
        return findByINodeId(params);
      case ByINodeIds:
        return findByINodeIds(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(), getAdded(), getModified());
  }

  @Override
  HopUnderReplicatedBlock cloneEntity(
      HopUnderReplicatedBlock hopUnderReplicatedBlock) {
    return cloneEntity(hopUnderReplicatedBlock, hopUnderReplicatedBlock
        .getInodeId());
  }

  @Override
  HopUnderReplicatedBlock cloneEntity(
      HopUnderReplicatedBlock hopUnderReplicatedBlock, int inodeId) {
    return new HopUnderReplicatedBlock(hopUnderReplicatedBlock.getLevel(),
        hopUnderReplicatedBlock.getBlockId(), inodeId);
  }

  @Override
  BlockPK getKey(HopUnderReplicatedBlock hopUnderReplicatedBlock) {
    return new BlockPK(hopUnderReplicatedBlock.getBlockId(),
        hopUnderReplicatedBlock.getInodeId());
  }

  private HopUnderReplicatedBlock findByBlockId(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    HopUnderReplicatedBlock result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      log("find-urblock-by-bid", CacheHitState.HIT,
          new String[]{"bid", Long.toString(blockId), "inode_id",
              Integer.toString(inodeId)});
      List<HopUnderReplicatedBlock> urblks = getByBlock(blockId);
      if (urblks != null) {
        if (urblks.size() > 1) {
          throw new IllegalStateException("you should have only one " +
              "UnderReplicatedBlock per block");
        }
        if(!urblks.isEmpty()) {
          result = urblks.get(0);
        }
      }
    } else {
      log("find-urblock-by-bid", CacheHitState.LOSS,
          new String[]{"bid", Long.toString(blockId), "inode_id",
              Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.findByPk(blockId, inodeId);
      gotFromDB(new BlockPK(blockId, inodeId), result);
    }
    return result;
  }

  private List<HopUnderReplicatedBlock> findByINodeId(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<HopUnderReplicatedBlock> result = null;
    if (containsByINode(inodeId)) {
      log("find-urblocks-by-inode-id", CacheHitState.HIT,
          new String[]{"inode_id", Integer.toString(inodeId)});
      result = getByINode(inodeId);
    } else {
      log("find-urblocks-by-inode-id", CacheHitState.LOSS,
          new String[]{"inode_id", Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.findByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
    }
    return result;
  }

  private List<HopUnderReplicatedBlock> findByINodeIds(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    List<HopUnderReplicatedBlock> result = null;
    log("find-urblocks-by-inode-ids", CacheHitState.LOSS,
        new String[]{"inode_ids", Arrays
            .toString(inodeIds)});
    aboutToAccessStorage();
    result = dataAccess.findByINodeIds(inodeIds);
    gotFromDB(BlockPK.getBlockKeys(inodeIds), result);
    return result;
  }

}
