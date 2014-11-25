/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.transaction.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HopsLeasePathLock extends HopsLock {

  private final TransactionLockTypes.LockType lockType;
  private final List<HopLeasePath> leasePaths;

  public HopsLeasePathLock(TransactionLockTypes.LockType lockType) {
    this.lockType = lockType;
    this.leasePaths = new ArrayList<HopLeasePath>();
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    HopsLeaseLock leaseLock = (HopsLeaseLock) locks.getLock(Type.Lease);
    for (Lease lease : leaseLock.getLeases()) {
      Collection<HopLeasePath> result = acquireLockList(lockType, HopLeasePath.Finder.ByHolderId, lease.getHolderID());
      if (!lease.getHolder().equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) { // We don't need to keep the lps result for namenode-lease. 
        leasePaths.addAll(result);
      }
    }
  }

  @Override
  Type getType() {
    return Type.LeasePath;
  }
  
  Collection<HopLeasePath> getLeasePaths(){
    return leasePaths;
  }
}
