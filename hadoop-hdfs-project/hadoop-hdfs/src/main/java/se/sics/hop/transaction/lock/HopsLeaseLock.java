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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.Lease;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public final class HopsLeaseLock extends HopsLock {

  private final TransactionLockTypes.LockType lockType;
  private final String leaseHolder;
  private final List<Lease> leases;

  HopsLeaseLock(TransactionLockTypes.LockType lockType, String leaseHolder) {
    this.lockType = lockType;
    this.leaseHolder = leaseHolder;
    this.leases = new ArrayList<Lease>();
  }

  HopsLeaseLock(TransactionLockTypes.LockType lockType) {
    this(lockType, null);
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    Set<String> hldrs = new HashSet<String>();
    if (leaseHolder != null) {
      hldrs.add(leaseHolder);
    }

    if (locks.containsLock(Type.INode)) {
      HopsBaseINodeLock inodeLock = (HopsBaseINodeLock) locks.getLock(Type.INode);

      for (INode f : inodeLock.getAllResolvedINodes()) {
        if (f instanceof INodeFileUnderConstruction) {
          hldrs.add(((INodeFileUnderConstruction) f).getClientName());
        }
      }
    }
    
    List<String> holders = new ArrayList<String>(hldrs);
    Collections.sort(holders);
    
    for (String h : holders) {
      Lease lease = acquireLock(lockType, Lease.Finder.ByHolder, h);
      if (lease != null) {
        leases.add(lease);
      }
    }
  }

  Collection<Lease> getLeases() {
    return leases;
  }

  public TransactionLockTypes.LockType getLockType() {
    return lockType;
  }

  @Override
  protected final Type getType() {
    return Type.Lease;
  }
}
