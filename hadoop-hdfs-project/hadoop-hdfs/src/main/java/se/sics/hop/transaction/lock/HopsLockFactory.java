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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HopsLockFactory {

  private final static HopsLockFactory instance = new HopsLockFactory();

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
  
  public HopsLock getBlockLock(long blockId, INodeIdentifier inode) {
    return new HopsBlockLock(blockId, inode);
  }
   
  public HopsLock getReplicaLock() {
    return new HopsBlockRelatedLock(HopsLock.Type.Replica);
  }

  public HopsLock getCorruptReplicaLock() {
    return new HopsBlockRelatedLock(HopsLock.Type.CorruptReplica);
  }

  public HopsLock getExcessReplicaLock() {
    return new HopsBlockRelatedLock(HopsLock.Type.ExcessReplica);
  }

  public HopsLock getReplicatUnderConstructionLock() {
    return new HopsBlockRelatedLock(HopsLock.Type.ReplicaUnderConstruction);
  }

  public HopsLock getInvalidatedBlockLock() {
    return new HopsBlockRelatedLock(HopsLock.Type.InvalidatedBlock);
  }

  public HopsLock getUnderReplicatedBlockLock() {
    return new HopsBlockRelatedLock(HopsLock.Type.UnderReplicatedBlock);
  }

  public HopsLock getPendingBlockLock() {
    return new HopsBlockRelatedLock(HopsLock.Type.PendingBlock);
  }

  public HopsLock getSqlBatchedBlocksLock() {
    return new HopsSqlBatchedBlocksLock();
  }

  public HopsLock getSqlBatchedReplicasLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopsLock.Type.Replica);
  }

  public HopsLock getSqlBatchedCorruptReplicasLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopsLock.Type.CorruptReplica);
  }

  public HopsLock getSqlBatchedExcessReplicasLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopsLock.Type.ExcessReplica);
  }

  public HopsLock getSqlBatchedReplicasUnderConstructionLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopsLock.Type.ReplicaUnderConstruction);
  }

  public HopsLock getSqlBatchedInvalidatedBlocksLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopsLock.Type.InvalidatedBlock);
  }

  public HopsLock getSqlBatchedUnderReplicatedBlocksLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopsLock.Type.UnderReplicatedBlock);
  }

  public HopsLock getSqlBatchedPendingBlocksLock() {
    return new HopsSqlBatchedBlocksRelatedLock(HopsLock.Type.PendingBlock);
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

  public HopsLock getINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
      boolean ignoreLocalSubtreeLocks, String... paths) {
    return new HopsINodeLock(lockType, resolveType, resolveLink,
        ignoreLocalSubtreeLocks, nameNode.getId(),
        nameNode.getActiveNamenodes().getActiveNamenodes(), paths);
  }

  public HopsLock getINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, boolean resolveLink,
      String... paths) {
    return new HopsINodeLock(lockType, resolveType, resolveLink,
        nameNode.getActiveNamenodes().getActiveNamenodes(), paths);
  }

  public HopsLock getINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String... paths) {
    return new HopsINodeLock(lockType, resolveType,
        nameNode.getActiveNamenodes().getActiveNamenodes(), paths);
  }

  public HopsLock getRenameINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      boolean ignoreLocalSubtreeLocks, String src, String dst) {
    return new HopsRenameINodeLock(lockType, resolveType,
        ignoreLocalSubtreeLocks, nameNode.getId(),
        nameNode.getActiveNamenodes().getActiveNamenodes(), src, dst);
  }

  public HopsLock getRenameINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String src,
      String dst) {
    return new HopsRenameINodeLock(lockType, resolveType,
        nameNode.getActiveNamenodes().getActiveNamenodes(), src, dst);
  }

  public HopsLock getLegacyRenameINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType,
      boolean ignoreLocalSubtreeLocks, String src, String dst) {
    return new HopsRenameINodeLock(lockType, resolveType,
        ignoreLocalSubtreeLocks, nameNode.getId(),
        nameNode.getActiveNamenodes().getActiveNamenodes(), src, dst, true);
  }

  public HopsLock getLegacyRenameINodeLock(NameNode nameNode,
      TransactionLockTypes.INodeLockType lockType,
      TransactionLockTypes.INodeResolveType resolveType, String src,
      String dst) {
    return new HopsRenameINodeLock(lockType, resolveType,
        nameNode.getActiveNamenodes().getActiveNamenodes(), src, dst, true);
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

  public HopsLock getNameNodeLeaseLock(TransactionLockTypes.LockType lockType) {
    return new HopsNameNodeLeaseLock(lockType);
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
    
    
  public void setConfiguration(Configuration conf) {
    HopsLock.enableSetPartitionKey(conf.getBoolean(DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED, DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED_DEFAULT));
    HopsBaseINodeLock.setDefaultLockType(getPrecedingPathLockType(conf));
  }
  
  private TransactionLockTypes.INodeLockType getPrecedingPathLockType(Configuration conf) {
    String val = conf.get(DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE, DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE_DEFAULT);
    if (val.compareToIgnoreCase("READ") == 0) {
      return TransactionLockTypes.INodeLockType.READ;
    } else if (val.compareToIgnoreCase("READ_COMMITTED") == 0) {
      return TransactionLockTypes.INodeLockType.READ_COMMITTED;
    } else {
      throw new IllegalStateException("Critical Parameter is not defined. Set " + DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE);
    }
  }
}
