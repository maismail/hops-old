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
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockInfoContext extends BaseEntityContext<Long, BlockInfo> {

  private final static int DEFAULT_NUM_BLOCKS_PER_INODE = 10;
  private final static Comparator<BlockInfo> BLOCK_INDEX_COMPARTOR = new
      Comparator<BlockInfo>() {
        @Override
        public int compare(BlockInfo o1,
            BlockInfo o2) {
          return Ints.compare(o1.getBlockIndex(), o2.getBlockIndex());
        }
      };

  private final Map<Integer, List<BlockInfo>> inodeBlocks = new HashMap<Integer,
      List<BlockInfo>>();
  private final List<BlockInfo> concatRemovedBlks = new ArrayList<BlockInfo>();

  private final BlockInfoDataAccess<BlockInfo> dataAccess;

  public BlockInfoContext(BlockInfoDataAccess<BlockInfo> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodeBlocks.clear();
    concatRemovedBlks.clear();
  }

  @Override
  public void update(BlockInfo blockInfo) throws TransactionContextException {
    super.update(blockInfo);
    //only called in update not add
    updateInodeBlocks(blockInfo);
    log("updated-blockinfo", CacheHitState.NA,
        new String[]{"bid", Long.toString(blockInfo.getBlockId()), "inodeId",
            Long.toString(blockInfo.getInodeId()), "blk index",
            Integer.toString(blockInfo.getBlockIndex())});

  }

  @Override
  public void remove(BlockInfo blockInfo) throws TransactionContextException {
    super.remove(blockInfo);
    removeBlockFromInodeBlocks(blockInfo);
    log("removed-blockinfo", CacheHitState.NA,
        new String[]{"bid", Long.toString(blockInfo.getBlockId())});
  }


  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    Collection<BlockInfo> removed = new ArrayList<BlockInfo>(getRemoved());
    removed.addAll(concatRemovedBlks);
    dataAccess.prepare(removed, getAdded(), getModified());
  }

  @Override
  public BlockInfo find(FinderType<BlockInfo> finder, Object... params)
      throws TransactionContextException, StorageException {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    switch (bFinder) {
      case ById:
        return findById(params);
      case MAX_BLK_INDX:
        return findMaxBlk(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<BlockInfo> findList(FinderType<BlockInfo> finder,
      Object... params) throws TransactionContextException, StorageException {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    switch (bFinder) {
      case ByInodeId:
        return findByInodeId(params);
      case ByIds:
        return findBatch(params);
      case ByInodeIds:
        return findByInodeIds(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HOPTransactionContextMaintenanceCmds hopCmds =
        (HOPTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        //delete the previous row from db
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        break;
      case Concat:
        //checkForSnapshotChange();
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK) params[0];
        List<HopINodeCandidatePK> srcs_param =
            (List<HopINodeCandidatePK>) params[1]; // these are the merged inodes
        List<BlockInfo> oldBlks = (List<BlockInfo>) params[2];
        deleteBlocksForConcat(trg_param, srcs_param, oldBlks);
        //new blocks have been added by the concat function
        //we just have to delete the blocks rows that dont make sence

        break;
    }
  }

  @Override
  Long getKey(BlockInfo blockInfo) {
    return blockInfo.getBlockId();
  }


  private List<BlockInfo> findByInodeId(final Object[] params) throws
      TransactionContextException,
      StorageException {
    List<BlockInfo> result = null;
    final Integer inodeId = (Integer) params[0];
    if (inodeBlocks.containsKey(inodeId)) {
      log("find-blocks-by-inodeid", CacheHitState.HIT,
          new String[]{"inodeid", Integer.toString(inodeId)});
      return inodeBlocks.get(inodeId);
    } else {
      log("find-blocks-by-inodeid", CacheHitState.LOSS,
          new String[]{"inodeid", Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.findByInodeId(inodeId);
      inodeBlocks.put(inodeId, syncBlockInfoInstances(result));
      return result;
    }
  }

  private List<BlockInfo> findBatch(Object[] params) throws
      TransactionContextException,
      StorageException {
    List<BlockInfo> result = null;
    final long[] blockIds = (long[]) params[0];
    final int[] inodeIds = (int[]) params[1];
    log("find-blocks-by-ids", CacheHitState.NA,
        new String[]{"BlockIds", "" + blockIds, "InodeIds", "" + inodeIds});
    aboutToAccessStorage();
    result = dataAccess.findByIds(blockIds, inodeIds);
    return syncBlockInfoInstances(result, blockIds);
  }

  private List<BlockInfo> findByInodeIds(Object[] params) throws
      TransactionContextException,
      StorageException {
    List<BlockInfo> result = null;
    final int[] ids = (int[]) params[0];
    log("find-blocks-by-inodeids", CacheHitState.NA,
        new String[]{"InodeIds", "" + ids});
    aboutToAccessStorage();
    result = dataAccess.findByInodeIds(ids);
    for (int id : ids) {
      inodeBlocks.put(id, null);
    }
    return syncBlockInfoInstances(result, true);
  }

  private BlockInfo findById(final Object[] params)
      throws TransactionContextException,
      StorageException {
    BlockInfo result = null;
    long blockId = (Long) params[0];
    Integer inodeId = null;
    if (params.length > 1 && params[1] != null) {
      inodeId = (Integer) params[1];
    }
    if (contains(blockId)) {
      result = get(blockId);
      log("find-block-by-bid", CacheHitState.HIT, new String[]{"bid", Long
          .toString(blockId), "inodeId",
          inodeId != null ? Integer.toString(inodeId)
              : "NULL"});
    } else {
      // some test intentionally look for blocks that are not in the DB
      // duing the acquire lock phase if we see that an id does not
      // exist in the db then we should put null in the cache for that id

      log("find-block-by-bid", CacheHitState.LOSS, new String[]{"bid", Long
          .toString(blockId), "inodeId",
          inodeId != null ? Integer.toString(inodeId)
              : "NULL"});
      aboutToAccessStorage();
      if (inodeId == null) {
        throw new IllegalArgumentException("InodeId is not set");
      }
      result = dataAccess.findById(blockId, inodeId);
      gotFromDB(blockId, result);
    }
    return result;
  }

  private BlockInfo findMaxBlk(final Object[] params) {
    final int inodeId = (Integer) params[0];
    Collection<BlockInfo> notRemovedBlks = Collections2.filter
        (filterValuesNotOnState
            (State
                .REMOVED), new
            Predicate<BlockInfo>() {
              @Override
              public boolean apply(BlockInfo input) {
                return input.getInodeId() == inodeId;
              }
            });
    return Collections.max(notRemovedBlks, BLOCK_INDEX_COMPARTOR);
  }


  private List<BlockInfo> syncBlockInfoInstances(List<BlockInfo> newBlocks,
      long[] blockIds) {
    List<BlockInfo> result = syncBlockInfoInstances(newBlocks);
    for (long blockId : blockIds) {
      if (!contains(blockId)) {
        gotFromDB(blockId, null);
      }
    }
    return result;
  }

  private List<BlockInfo> syncBlockInfoInstances(List<BlockInfo> newBlocks) {
    return syncBlockInfoInstances(newBlocks, false);
  }

  private List<BlockInfo> syncBlockInfoInstances(List<BlockInfo> newBlocks,
      boolean syncInodeBlocks) {
    List<BlockInfo> finalList = new ArrayList<BlockInfo>();

    for (BlockInfo blockInfo : newBlocks) {
      if (isRemoved(blockInfo.getBlockId())) {
        continue;
      }

      gotFromDB(blockInfo);
      finalList.add(blockInfo);

      if (syncInodeBlocks) {
        List<BlockInfo> blockList = inodeBlocks.get(blockInfo.getInodeId());
        if (blockList == null) {
          blockList = new ArrayList<BlockInfo>();
          inodeBlocks.put(blockInfo.getInodeId(), blockList);
        }
        blockList.add(blockInfo);
      }
    }

    return finalList;
  }

  private void updateInodeBlocks(BlockInfo newBlock) {
    List<BlockInfo> blockList = inodeBlocks.get(newBlock.getInodeId());

    if (blockList != null) {
      int idx = blockList.indexOf(newBlock);
      if (idx != -1) {
        blockList.set(idx, newBlock);
      } else {
        blockList.add(newBlock);
      }
    } else {
      List<BlockInfo> list =
          new ArrayList<BlockInfo>(DEFAULT_NUM_BLOCKS_PER_INODE);
      list.add(newBlock);
      inodeBlocks.put(newBlock.getInodeId(), list);
    }
  }

  private void removeBlockFromInodeBlocks(BlockInfo block)
      throws TransactionContextException {
    List<BlockInfo> blockList = inodeBlocks.get(block.getInodeId());
    if (blockList != null) {
      if (!blockList.remove(block)) {
        throw new TransactionContextException(
            "Trying to remove a block that does not exist");
      }
    }
  }

  private void checkForSnapshotChange() {
    // when you overwrite a file the dst file blocks are removed
    // removedBlocks list may not be empty
    if (!getAdded().isEmpty() || !getModified().isEmpty()) {//incase of move and
      // rename the
      // blocks should not have been modified in any way
      throw new IllegalStateException(
          "Renaming a file(s) whose blocks are changed. During rename and move no block blocks should have been changed.");
    }
  }

  private void deleteBlocksForConcat(HopINodeCandidatePK trg_param,
      List<HopINodeCandidatePK> deleteINodes, List<BlockInfo> oldBlks /* blks with old pk*/)
      throws TransactionContextException {

    if (!getRemoved()
        .isEmpty()) {//in case of concat new block_infos rows are added by
      // the concat fn
      throw new IllegalStateException(
          "Concat file(s) whose blocks are changed. During rename and move no block blocks should have been changed.");
    }

    for (BlockInfo bInfo : oldBlks) {
      HopINodeCandidatePK pk = new HopINodeCandidatePK(bInfo.getInodeId());
      if (deleteINodes.contains(pk)) {
        //remove the block
        concatRemovedBlks.add(bInfo);
        log("snapshot-maintenance-removed-blockinfo", CacheHitState.NA,
            new String[]{"bid", Long.toString(bInfo.getBlockId()), "inodeId",
                Integer.toString(bInfo.getInodeId())});
      }
    }
  }

}
