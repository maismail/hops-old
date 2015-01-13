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

import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.HashMap;
import java.util.Map;

public class EncodingStatusContext extends BaseEntityContext<Integer,
    EncodingStatus> {

  private final EncodingStatusDataAccess<EncodingStatus> dataAccess;
  private final Map<Integer, EncodingStatus>
      parityInodeIdToEncodingStatus = new HashMap<Integer, EncodingStatus>();

  public EncodingStatusContext(
      EncodingStatusDataAccess<EncodingStatus> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(EncodingStatus encodingStatus)
      throws TransactionContextException {
    super.update(encodingStatus);
    addInternal(encodingStatus);
  }

  @Override
  public void remove(EncodingStatus encodingStatus)
      throws TransactionContextException {
    if(!contains(encodingStatus.getInodeId())){
      update(encodingStatus);
    }
    super.remove(encodingStatus);
    removeInternal(encodingStatus);
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    parityInodeIdToEncodingStatus.clear();
  }

  @Override
  public EncodingStatus find(FinderType<EncodingStatus> finder,
      Object... params) throws TransactionContextException, StorageException {
    EncodingStatus.Finder eFinder = (EncodingStatus.Finder) finder;

    Integer inodeId = (Integer) params[0];
    if (inodeId == null) {
      return null;
    }

    switch (eFinder) {
      case ByInodeId:
        return findByINodeId(inodeId);
      case ByParityInodeId:
        return findByParityINodeId(inodeId);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public int count(CounterType<EncodingStatus> counter, Object... params)
      throws TransactionContextException, StorageException {
    EncodingStatus.Counter eCounter = (EncodingStatus.Counter) counter;
    switch (eCounter) {
      case RequestedEncodings:
        return dataAccess.countRequestedEncodings();
      case ActiveEncodings:
        return dataAccess.countActiveEncodings();
      case ActiveRepairs:
        return dataAccess.countActiveRepairs();
      case Encoded:
        return dataAccess.countEncoded();
      default:
        throw new RuntimeException(UNSUPPORTED_COUNTER);
    }
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    for (EncodingStatus status : getAdded()) {
      dataAccess.add(status);
    }

    for (EncodingStatus status : getModified()) {
      dataAccess.update(status);
    }

    for (EncodingStatus status : getRemoved()) {
      dataAccess.delete(status);
    }
  }

  @Override
  Integer getKey(EncodingStatus encodingStatus) {
    return encodingStatus.getInodeId();
  }

  private EncodingStatus findByINodeId(final int inodeId)
      throws StorageCallPreventedException, StorageException {
    EncodingStatus result = null;
    if (contains(inodeId)) {
      log("find-encoding-status-by-inodeid", CacheHitState.HIT,
          new String[]{"inodeid", Integer.toString(inodeId)});
      result = get(inodeId);
    } else {
      log("find-encoding-status-by-inodeid", CacheHitState.LOSS,
          new String[]{"inodeid", Integer.toString(inodeId)});
      aboutToAccessStorage();
      result = dataAccess.findByInodeId(inodeId);
      gotFromDB(inodeId, result);
      addInternal(result);
    }
    return result;
  }

  private EncodingStatus findByParityINodeId(final int pairtyINodeId)
      throws StorageCallPreventedException, StorageException {
    EncodingStatus result = null;
    if (parityInodeIdToEncodingStatus.containsKey(pairtyINodeId)) {
      log("find-encoding-status-by-parityInodeid", CacheHitState.HIT,
          new String[]{"inodeid", Integer.toString(pairtyINodeId)});
      result = parityInodeIdToEncodingStatus.get(pairtyINodeId);
    } else {
      log("find-encoding-status-by-parityInodeid", CacheHitState.LOSS,
          new String[]{"inodeid", Integer.toString(pairtyINodeId)});
      aboutToAccessStorage();
      result = dataAccess.findByParityInodeId(pairtyINodeId);
      gotFromDB(result);
      addInternal(pairtyINodeId, result);
    }
    return result;
  }


  private void addInternal(EncodingStatus encodingStatus) {
    if (encodingStatus != null && encodingStatus.getParityInodeId() != null) {
      addInternal(encodingStatus.getParityInodeId(), encodingStatus);
    }
  }

  private void addInternal(int parityINodeId, EncodingStatus encodingStatus) {
    parityInodeIdToEncodingStatus.put(parityINodeId, encodingStatus);
  }

  private void removeInternal(EncodingStatus encodingStatus) {
    if (encodingStatus != null && encodingStatus.getParityInodeId() != null) {
      parityInodeIdToEncodingStatus.remove(encodingStatus.getParityInodeId());
    }
  }

}
