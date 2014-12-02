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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
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
  void acquire(TransactionLocks locks) throws Exception {
    HopsINodeLock inodeLock = (HopsINodeLock) locks.getLock(Type.INode);

    // TODO This sorts after each insertion what is inefficient
    SortedSet<String> holders = new TreeSet<String>();
    if (leaseHolder != null) {
      holders.add(leaseHolder);
    }

    for (INode f : inodeLock.getAllResolvedINodes()) {
      if (f instanceof INodeFileUnderConstruction) {
        holders.add(((INodeFileUnderConstruction) f).getClientName());
      }
    }

    for (String h : holders) {
      Lease lease = acquireLock(lockType, Lease.Finder.ByPKey, h);
      if (lease != null) {
        leases.add(lease);
      }
    }
  }

  Collection<Lease> getLeases() {
    return leases;
  }

  public TransactionLockTypes.LockType  getLockType(){
    return lockType;
  }
  
  @Override
  final Type getType() {
    return Type.Lease;
  }
}
