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
package se.sics.hop.transaction.lock;

import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.LeaderElection;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import se.sics.hop.transaction.EntityManager;

final class HopsLeaderLock extends HopsLock {
  private final TransactionLockTypes.LockType lockType;

  HopsLeaderLock(TransactionLockTypes.LockType lockType) {
    this.lockType = lockType;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws Exception {
    setPartitioningKey();
    acquireLockList(lockType, HopLeader.Finder.All);
  }

  private void setPartitioningKey() throws StorageException {
    if (isSetPartitionKeyEnabled()) {
      Object[] key = new Object[2];
      key[0] = LeaderElection.LEADER_INITIALIZATION_ID;
      key[1] = HopLeader.DEFAULT_PARTITION_VALUE;
      EntityManager.setPartitionKey(LeaderDataAccess.class, key);
    }
  }

  @Override
  protected final Type getType() {
    return Type.LeaderLock;
  }
  
  TransactionLockTypes.LockType getLockType(){
    return lockType;
  }
}
