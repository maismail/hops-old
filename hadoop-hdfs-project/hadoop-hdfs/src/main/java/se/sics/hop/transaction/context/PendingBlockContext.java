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


import com.google.common.primitives.Ints;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class PendingBlockContext extends
    BaseReplicaContext<BlockPK
        , PendingBlockInfo> {

  private final PendingBlockDataAccess<PendingBlockInfo> dataAccess;
  private boolean allPendingRead = false;

  public PendingBlockContext(
      PendingBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(PendingBlockInfo pendingBlockInfo)
      throws TransactionContextException {
    super.update(pendingBlockInfo);
    log("added-pending", CacheHitState.NA,
        new String[]{"bid", Long.toString(pendingBlockInfo.getBlockId()),
            "numInProgress", Integer.toString(pendingBlockInfo.getNumReplicas())});
  }

  @Override
  public void remove(PendingBlockInfo pendingBlockInfo)
      throws TransactionContextException {
    super.remove(pendingBlockInfo);
    log("removed-pending", CacheHitState.NA, new String[]{"bid", Long.toString(pendingBlockInfo.getBlockId())});
  }

  @Override
  public PendingBlockInfo find(FinderType<PendingBlockInfo> finder,
      Object... params) throws TransactionContextException, StorageException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    switch (pFinder) {
      case ByBlockId: return findByBlockId(params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<PendingBlockInfo> findList(
      FinderType<PendingBlockInfo> finder, Object... params)
      throws TransactionContextException, StorageException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    switch (pFinder) {
      case All: return findAll();
      case ByInodeId: return findByINodeId(params);
      case ByInodeIds: return findByINodeIds(params);
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
    allPendingRead = false;
  }

  @Override
  PendingBlockInfo cloneEntity(PendingBlockInfo pendingBlockInfo) {
    return cloneEntity(pendingBlockInfo, pendingBlockInfo.getInodeId());
  }

  @Override
  PendingBlockInfo cloneEntity(PendingBlockInfo pendingBlockInfo, int inodeId) {
    return new PendingBlockInfo(pendingBlockInfo.getBlockId(),inodeId,
        pendingBlockInfo.getTimeStamp(),pendingBlockInfo.getNumReplicas());
  }

  @Override
  BlockPK getKey(PendingBlockInfo pendingBlockInfo) {
    return new BlockPK(pendingBlockInfo.getBlockId(),
        pendingBlockInfo.getInodeId());
  }

  private PendingBlockInfo findByBlockId(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final long blockId = (Long) params[0];
    final int inodeId = (Integer) params[1];
    PendingBlockInfo result = null;
    if (containsByBlock(blockId) || containsByINode(inodeId)) {
      log("find-pending-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId),"inode_id", Integer.toString(inodeId)});
      List<PendingBlockInfo> pblks = getByBlock(blockId);
      if (pblks != null) {
        if (pblks.size() > 1) {
          throw new IllegalStateException("you should have only one " +
              "PendingBlockInfo per block");
        }
        if(!pblks.isEmpty()){
          result = pblks.get(0);
        }
    }
    } else {
      log("find-pending-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId),"inode_id", Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.findByPKey(blockId,inodeId);
      gotFromDB(new BlockPK(blockId, inodeId), result);
    }
    return result;
  }

  private  List<PendingBlockInfo> findAll()
      throws StorageCallPreventedException, StorageException {
    List<PendingBlockInfo> result = null;
    if (allPendingRead) {
      log("find-all-pendings", CacheHitState.HIT);
      result = new ArrayList<PendingBlockInfo>(getAll());
    } else {
      log("find-all-pendings", CacheHitState.LOSS);
      aboutToAccessStorage();
      result = dataAccess.findAll();
      gotFromDB(result);
      allPendingRead = true;
    }
    return result;
  }

  private List<PendingBlockInfo> findByINodeId(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    List<PendingBlockInfo> result = null;
    if(containsByINode(inodeId)){
      log("find-pendings-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId)});
      result = getByINode(inodeId);
    }else{
      log("find-pendings-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.findByINodeId(inodeId);
      gotFromDB(new BlockPK(inodeId), result);
    }
    return result;
  }

  private List<PendingBlockInfo> findByINodeIds(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int[] inodeIds = (int[]) params[0];
    List<PendingBlockInfo> result = null;
    log("find-pendings-by-inode-ids", CacheHitState.LOSS, new String[]{"inode_ids", Arrays
        .toString(inodeIds)});
    aboutToAccessStorage();
    result = dataAccess.findByINodeIds(inodeIds);
    gotFromDB(BlockPK.getBlockKeys(inodeIds), result);
    return result;
  }
}
