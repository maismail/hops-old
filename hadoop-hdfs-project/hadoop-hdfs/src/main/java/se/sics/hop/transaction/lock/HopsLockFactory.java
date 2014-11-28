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

import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

public class HopsLockFactory {
  private static final HopsLockFactory instance = new HopsLockFactory();

  private HopsLockFactory() {

  }

  public static HopsLockFactory getInstance() {
    return instance;
  }

  public HopsLock getBlockChecksumLock(String target, int blockIndex) {
    return new HopsBlockChecksumLock(target, blockIndex);
  }

  public HopsLock getBlockLock() {
    return new HopsBlockLock();
  }

  public HopsLock getReplicaLock() {
    return new HopsBlockRelatedLock.HopsReplicaLock();
  }

  public HopsLock getCorruptReplicaLock() {
    return new HopsBlockRelatedLock.HopsCorruptReplicaLock();
  }

  public HopsLock getExcessReplicaLock() {
    return new HopsBlockRelatedLock.HopsExcessReplicaLock();
  }

  public HopsLock getReplicatUnderConstructionLock() {
    return new HopsBlockRelatedLock.HopsReplicatUnderConstructionLock();
  }

  public HopsLock getInvalidatedBlockLock() {
    return new HopsBlockRelatedLock.HopsInvalidatedBlockLock();
  }

  public HopsLock getUnderReplicatedBlockLock() {
    return new HopsBlockRelatedLock.HopsUnderReplicatedBlockLock();
  }

  public HopsLock getPendingBlockLock() {
    return new HopsBlockRelatedLock.HopsPendingBlockLock();
  }

  public Collection<HopsLock> getBlockRelatedLocks() {
    ArrayList<HopsLock> list = new ArrayList(8);
    list.add(getBlockLock());
    list.add(getReplicaLock());
    list.add(getCorruptReplicaLock());
    list.add(getExcessReplicaLock());
    list.add(getReplicatUnderConstructionLock());
    list.add(getInvalidatedBlockLock());
    list.add(getUnderReplicatedBlockLock());
    list.add(getPendingBlockLock());
    return list;
  }

  public HopsLock getIndividualBlockLock(long blockId, INodeIdentifier inode) {
    return new HopsIndividualBlockLock(blockId, inode);
  }

  public HopsLock getIndividualINodeLock(
      TransactionLockTypes.INodeLockType lockType,
      INodeIdentifier inodeIdentifier, boolean readUpPathInodes) {
    return new HopsIndividualINodeLock(lockType, inodeIdentifier,
        readUpPathInodes);
  }

  public HopsLock getIndividualINodeLock(
      TransactionLockTypes.INodeLockType lockType,
      INodeIdentifier inodeIdentifier) {
    return new HopsIndividualINodeLock(lockType, inodeIdentifier);
  }

  public HopsLock getINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
      boolean ignoreLocalSubtreeLocks, long namenodeId,
      List<ActiveNamenode> activeNamenodes, String... paths) {
    return new HopsINodeLock(lockType, resolveType, resolveLink,
        ignoreLocalSubtreeLocks, namenodeId, activeNamenodes, paths);
  }

  public HopsLock getINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
      String... paths) {
    return new HopsINodeLock(lockType, resolveType, resolveLink, paths);
  }

  public HopsLock getLeaderLock(TransactionLockTypes.LockType lockType) {
    return new HopsLeaderLock(lockType);
  }

  public HopsLock getLeaseLock(TransactionLockTypes.LockType lockType,
      String leaseHolder) {
    return new HopsLeaseLock(lockType, leaseHolder);
  }

  public HopsLock getLeaseLock(TransactionLockTypes.LockType lockType) {
    return new HopsLeaseLock(lockType);
  }

  public HopsLock getLeasePathLock(TransactionLockTypes.LockType lockType,
      int expectedCount) {
    return new HopsLeasePathLock(lockType, expectedCount);
  }

  public HopsLock getLeasePathLock(TransactionLockTypes.LockType lockType) {
    return new HopsLeasePathLock(lockType);
  }

  public HopsLock getNameNodeLeaseLock(TransactionLockTypes.LockType lockType,
      SortedSet<String> paths) {
    return new HopsNameNodeLeaseLock(lockType, paths);
  }

  public HopsLock getQuotaUpdateLock(boolean includeChildren,
      String... targets) {
    return new HopsQuotaUpdateLock(includeChildren, targets);
  }

  public HopsLock getQuotaUpdateLock(String... targets) {
    return new HopsQuotaUpdateLock(targets);
  }

  public HopsLock getVariableLock(HopVariable.Finder[] finders,
      TransactionLockTypes.LockType[] lockTypes) {
    assert finders.length == lockTypes.length;
    HopsVariablesLock lock = new HopsVariablesLock();
    for (int i = 0; i < finders.length; i++) {
      lock.addVariable(finders[i], lockTypes[i]);
    }
    return lock;
  }
}
