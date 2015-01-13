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
import se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hop.BlockChecksum;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess.KeyTuple;

public class BlockChecksumContext extends
    BaseEntityContext<KeyTuple, BlockChecksum> {

  private final BlockChecksumDataAccess<BlockChecksum> dataAccess;
  private final Map<Integer, Collection<BlockChecksum>> inodeToBlockChecksums =
      new
          HashMap<Integer, Collection<BlockChecksum>>();

  public BlockChecksumContext(
      BlockChecksumDataAccess<BlockChecksum> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(BlockChecksum blockChecksum)
      throws TransactionContextException {
    super.update(blockChecksum);
  }

  @Override
  public void remove(BlockChecksum blockChecksum)
      throws TransactionContextException {
    super.remove(blockChecksum);
  }

  @Override
  public BlockChecksum find(FinderType<BlockChecksum> finder, Object... params)
      throws TransactionContextException, StorageException {
    BlockChecksum.Finder eFinder = (BlockChecksum.Finder) finder;
    switch (eFinder) {
      case ByKeyTuple:
        return findByKeyTuple(params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public Collection<BlockChecksum> findList(FinderType<BlockChecksum> finder,
      Object... params) throws TransactionContextException, StorageException {
    BlockChecksum.Finder eFinder = (BlockChecksum.Finder) finder;
    switch (eFinder) {
      case ByInodeId:
        return findByINodeId(params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    for (BlockChecksum blockChecksum : getAdded()) {
      dataAccess.add(blockChecksum);
    }
    for (BlockChecksum blockChecksum : getModified()) {
      dataAccess.update(blockChecksum);
    }
    for (BlockChecksum blockChecksum : getRemoved()) {
      dataAccess.delete(blockChecksum);
    }
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodeToBlockChecksums.clear();
  }

  @Override
  KeyTuple getKey(BlockChecksum blockChecksum) {
    return new KeyTuple(blockChecksum.getInodeId(), blockChecksum
        .getBlockIndex());
  }

  private BlockChecksum findByKeyTuple(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final KeyTuple key = (KeyTuple) params[0];
    if (key == null) {
      return null;
    }
    BlockChecksum result = null;
    if (contains(key)) {
      log("find-block-checksum-by-keyTuple", CacheHitState.HIT,
          new String[]{"KeyTuple", key.toString()});
      result = get(key);
    } else {
      log("find-block-checksum-by-keyTuple", CacheHitState.LOSS,
          new String[]{"KeyTuple", key.toString()});
      aboutToAccessStorage();
      result = dataAccess.find(key.getInodeId(), key.getBlockIndex());
      gotFromDB(key, result);
    }
    return result;
  }

  private Collection<BlockChecksum> findByINodeId(Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    Collection<BlockChecksum> result = null;
    if (inodeToBlockChecksums.containsKey(inodeId)) {
      result = inodeToBlockChecksums.get(inodeId);
    } else {
      aboutToAccessStorage();
      result = dataAccess.findAll((Integer) params[0]);
      gotFromDB(result);
      inodeToBlockChecksums.put(inodeId, result);
    }
    return result;
  }
}
