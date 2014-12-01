/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package se.sics.hop.transaction.lock;

import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;

public class HopsLockFactory {

  public static enum BLK{
    /**
     * Replica
     */
    RE, 
    /**
     * CorruptReplica
     */
    CR, 
    /**
     * ExcessReplica
     */
    ER,
    /**
     * UnderReplicated
     */
    UR,
    /**
     * ReplicaUnderConstruction
     */
    UC,
    /**
     * InvalidatedBlock
     */
    IV,
    /**
     * PendingBlock
     */
    PE
  }
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
    return new HopsBlockRelatedLock(HopIndexedReplica.Finder.ByBlockId, HopsLock.Type.Replica);
  }

  public HopsLock getCorruptReplicaLock() {
    return new HopsBlockRelatedLock(HopCorruptReplica.Finder.ByBlockId, HopsLock.Type.CorruptReplica);
  }

  public HopsLock getExcessReplicaLock() {
    return new HopsBlockRelatedLock(HopExcessReplica.Finder.ByBlockId, HopsLock.Type.ExcessReplica);
  }

  public HopsLock getReplicatUnderConstructionLock() {
    return new HopsBlockRelatedLock(ReplicaUnderConstruction.Finder.ByBlockId, HopsLock.Type.ReplicaUnderConstruction);
  }

  public HopsLock getInvalidatedBlockLock() {
    return new HopsBlockRelatedLock(HopInvalidatedBlock.Finder.ByBlockId, HopsLock.Type.InvalidatedBlock);
  }

  public HopsLock getUnderReplicatedBlockLock() {
    return new HopsBlockRelatedLock(false, HopUnderReplicatedBlock.Finder.ByBlockId, HopsLock.Type.UnderReplicatedBlock);
  }

  public HopsLock getPendingBlockLock() {
    return new HopsBlockRelatedLock(false, PendingBlockInfo.Finder.ByBlockId, HopsLock.Type.PendingBlock);
  }

  public HopsLock getSqlBatchedBlocksLock() {
    return new HopsSqlBatchedBlocksLock();
  }

  public HopsLock getSqlBatchedReplicasLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopIndexedReplica.Finder.ByINodeIds, HopsLock.Type.Replica);
  }

  public HopsLock getSqlBatchedCorruptReplicasLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopCorruptReplica.Finder.ByINodeIds, HopsLock.Type.CorruptReplica);
  }

  public HopsLock getSqlBatchedExcessReplicasLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopExcessReplica.Finder.ByINodeIds, HopsLock.Type.ExcessReplica);
  }

  public HopsLock getSqlBatchedReplicasUnderConstructionLock() {
    return new HopsSqlBatchedBlocksRelatedLock(ReplicaUnderConstruction.Finder.ByINodeIds, HopsLock.Type.ReplicaUnderConstruction);
  }

  public HopsLock getSqlBatchedInvalidatedBlocksLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopInvalidatedBlock.Finder.ByINodeIds, HopsLock.Type.InvalidatedBlock);
  }

  public HopsLock getSqlBatchedUnderReplicatedBlocksLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopUnderReplicatedBlock.Finder.ByINodeIds, HopsLock.Type.UnderReplicatedBlock);
  }

  public HopsLock getSqlBatchedPendingBlocksLock() {
    return new HopsSqlBatchedBlocksRelatedLock(PendingBlockInfo.Finder.ByInodeIds, HopsLock.Type.PendingBlock);
  }

  public HopsLock getIndividualBlockLock(long blockId, INodeIdentifier inode) {
    return new HopsIndividualBlockLock(blockId, inode);
  }

  public HopsLock getBatchedINodesLock(List<INodeIdentifier> inodeIdentifiers) {
    return new HopsBatchedINodeLock(inodeIdentifiers);
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

  public HopsLock getINodeLock(TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String... paths) {
    return new HopsINodeLock(lockType, resolveType, paths);
  }

  public HopsLock getRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
          TransactionLockTypes.INodeResolveType resolveType,
          boolean ignoreLocalSubtreeLocks, long namenodeId,
          List<ActiveNamenode> activeNamenodes, String src, String dst) {
    return new HopsRenameINodeLock(lockType, resolveType, ignoreLocalSubtreeLocks, namenodeId, activeNamenodes, src, dst);
  }

  public HopsLock getRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
          TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
          String src, String dst) {
    return new HopsRenameINodeLock(lockType, resolveType, src, dst);
  }

  public HopsLock getLegacyRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
          TransactionLockTypes.INodeResolveType resolveType,
          boolean ignoreLocalSubtreeLocks, long namenodeId,
          List<ActiveNamenode> activeNamenodes, String src, String dst) {
    return new HopsRenameINodeLock(lockType, resolveType, ignoreLocalSubtreeLocks, namenodeId, activeNamenodes, src, dst, true);
  }

  public HopsLock getLegacyRenameINodeLock(TransactionLockTypes.INodeLockType lockType,
          TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
          String src, String dst) {
    return new HopsRenameINodeLock(lockType, resolveType, src, dst, true);
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

  public HopsLock getVariableLock(HopVariable.Finder finder,
          TransactionLockTypes.LockType lockType) {
    HopsVariablesLock lock = new HopsVariablesLock();
    lock.addVariable(finder, lockType);
    return lock;
  }

  public List<HopsLock> getBlockReportingLocks(long[] blockIds, int storageId) {
    ArrayList<HopsLock> list = new ArrayList(3);
    list.add(new HopsBatchedBlockLock(blockIds));
    list.add(new HopsBatchedBlocksRelatedLock.HopsBatchedReplicasLock(storageId));
    list.add(new HopsBatchedBlocksRelatedLock.HopsBatchedInvalidatedBlocksLock(storageId));
    return list;
  }

  public HopsLock getEncodingStatusLock(TransactionLockTypes.LockType lockType, String... targets) {
    return new HopsBaseEncodingStatusLock.HopsEncodingStatusLock(lockType, targets);
  }

  public HopsLock getIndivdualEncodingStatusLock(TransactionLockTypes.LockType lockType, int inodeId) {
    return new HopsBaseEncodingStatusLock.HopsIndividualEncodingStatusLock(lockType, inodeId);
  }
  
  public Collection<HopsLock> getBlockRelated(BLK... relatedBlks) {
    ArrayList<HopsLock> list = new ArrayList();
    for (BLK b : relatedBlks) {
      switch (b) {
        case RE:
          list.add(getReplicaLock());
          break;
        case CR:
          list.add(getCorruptReplicaLock());
          break;
        case IV:
          list.add(getInvalidatedBlockLock());
          break;
        case PE:
          list.add(getPendingBlockLock());
          break;
        case UC:
          list.add(getReplicatUnderConstructionLock());
          break;
        case UR:
          list.add(getUnderReplicatedBlockLock());
          break;
        case ER:
          list.add(getExcessReplicaLock());
          break;
      }
    }
    return list;
  }
  
    public Collection<HopsLock> getSqlBatchedBlocksRelated(BLK... relatedBlks) {
    ArrayList<HopsLock> list = new ArrayList();
    for (BLK b : relatedBlks) {
      switch (b) {
        case RE:
          list.add(getSqlBatchedReplicasLock());
          break;
        case CR:
          list.add(getSqlBatchedCorruptReplicasLock());
          break;
        case IV:
          list.add(getSqlBatchedInvalidatedBlocksLock());
          break;
        case PE:
          list.add(getSqlBatchedInvalidatedBlocksLock());
          break;
        case UC:
          list.add(getSqlBatchedReplicasUnderConstructionLock());
          break;
        case UR:
          list.add(getUnderReplicatedBlockLock());
          break;
        case ER:
          list.add(getSqlBatchedExcessReplicasLock());
          break;
      }
    }
    return list;
  }
}
