package org.apache.hadoop.hdfs.server.namenode.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockTypes.*;

/**
 *
 * @author salman <salman@sics.se> && Hooman <hooman@sics.se>
 */
public class TransactionLocks {

  private final static Log LOG = LogFactory.getLog(TransactionLocks.class);
  //inode
  private INodeLockType inodeLock = null;
  private INodeResolveType inodeResolveType = null;
  private String[] inodeParam = null;
  private INode[] inodeResult = null;
  private boolean resolveLink = true; // the file is a symlink should it resolve it?
  protected LinkedList<INode> preTxResolvedInodes = null; // For the operations requires to have inodes before starting transactions.
  private HashMap<INode, INodeLockType> allLockedInodesInTx = new HashMap<INode, INodeLockType>();
  private boolean preTxPathFullyResolved;
  //block
  private LockType blockLock = null;
  private Long blockParam = null;
  // lease
  private LockType leaseLock = null;
  private String leaseParam = null;
  private LockType nnLeaseLock = null; // acquire lease for Name-node
  // lease paths
  private LockType lpLock = null;
  // replica
  private LockType replicaLock = null;
  // corrupt
  private LockType crLock = null;
  // excess
  private LockType erLock = null;
  //replica under contruction
  private LockType rucLock = null;
  // under replicated blocks
  private LockType urbLock = null;
  // pending blocks
  private LockType pbLock = null;
  // invalidated blocks
  private LockType invLocks = null;
  // block token key
  private LockType blockKeyLock = null;
  private List<Integer> blockKeyIds = null;
  private List<Short> blockKeyTypes = null;
  // block generation stamp
  private LockType generationStampLock = null;
  //block id counter 
  private LockType blockIdCounterLock = null;
  // Leader
  private LockType leaderLock = null;

  public String[] getInodeParam() {
    return inodeParam;
  }

  public INode[] getInodeResult() {
    return inodeResult;
  }

  public boolean isResolveLink() {
    return resolveLink;
  }

  public LinkedList<INode> getPreTxResolvedInodes() {
    return preTxResolvedInodes;
  }

  public String getLeaseParam() {
    return leaseParam;
  }

  public LockType getNnLeaseLock() {
    return nnLeaseLock;
  }

  public LockType getBlockKeyLock() {
    return blockKeyLock;
  }

  public List<Integer> getBlockKeyIds() {
    return blockKeyIds;
  }

  public List<Short> getBlockKeyTypes() {
    return blockKeyTypes;
  }

  TransactionLocks() {
  }

  TransactionLocks(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    this.preTxResolvedInodes = resolvedInodes;
    this.preTxPathFullyResolved = preTxPathFullyResolved;
  }

  public TransactionLocks addINode(INodeResolveType resolveType,
          INodeLockType lock, boolean resolveLink, String[] param) {
    this.inodeLock = lock;
    this.inodeResolveType = resolveType;
    this.inodeParam = param;
    this.resolveLink = resolveLink;
    return this;
  }

  public TransactionLocks addINode(INodeResolveType resolveType,
          INodeLockType lock, String[] param) {
    return addINode(resolveType, lock, true, param);
  }

  public TransactionLocks addINode(INodeLockType lock) {
    addINode(null, lock, null);
    return this;
  }

  public TransactionLocks addINode(INodeResolveType resolveType, INodeLockType lock) {
    return addINode(resolveType, lock, true, null);
  }

  public TransactionLocks addBlock(LockType lock, Long param) {
    this.blockLock = lock;
    this.blockParam = param;
    return this;
  }

  public TransactionLocks addBlock(LockType lock) {
    addBlock(lock, null);
    return this;
  }

  public TransactionLocks addLease(LockType lock, String param) {
    this.leaseLock = lock;
    this.leaseParam = param;
    return this;
  }

  public TransactionLocks addLease(LockType lock) {
    addLease(lock, null);
    return this;
  }

  public TransactionLocks addCorrupt(LockType lock) {
    this.crLock = lock;
    return this;
  }

  public TransactionLocks addExcess(LockType lock) {
    this.erLock = lock;
    return this;
  }

  public TransactionLocks addReplicaUc(LockType lock) {
    this.rucLock = lock;
    return this;
  }

  public TransactionLocks addReplica(LockType lock) {
    this.replicaLock = lock;
    return this;
  }

  public TransactionLocks addNameNodeLease(LockType lock) {
    this.nnLeaseLock = lock;
    return this;
  }

  public TransactionLocks addLeasePath(LockType lock) {
    this.lpLock = lock;
    return this;
  }

  public TransactionLocks addUnderReplicatedBlock(LockType lock) {
    this.urbLock = lock;
    return this;
  }

  public TransactionLocks addGenerationStamp(LockType lock) {
    this.generationStampLock = lock;
    return this;
  }

  public TransactionLocks addBlockIdCounter(LockType lock) {
    this.blockIdCounterLock = lock;
    return this;
  }

  /**
   * Lock on block token key data.
   *
   * @param lock
   * @param keyId
   * @return
   */
  public TransactionLocks addBlockKeyLockById(LockType lock, int keyId) {
    blockKeyLock = lock;
    if (blockKeyIds == null) {
      blockKeyIds = new ArrayList<Integer>();
    }
    blockKeyIds.add(keyId);
    return this;
  }

  public TransactionLocks addBlockKeyLockByType(LockType lock, short type) {
    blockKeyLock = lock;
    if (blockKeyTypes == null) {
      blockKeyTypes = new ArrayList<Short>();
    }
    blockKeyTypes.add(type);
    return this;
  }

  public TransactionLocks addLeaderLock(LockType lock, long... ids) {
    this.leaderLock = lock;
    return this;
  }

  public TransactionLocks addInvalidatedBlock(LockType lock) {
    this.invLocks = lock;
    return this;
  }

  public TransactionLocks addPendingBlock(LockType lock) {
    this.pbLock = lock;
    return this;
  }

  public INodeLockType getInodeLock() {
    return inodeLock;
  }

  public INodeResolveType getInodeResolveType() {
    return inodeResolveType;
  }

  public LockType getBlockLock() {
    return blockLock;
  }

  public Long getBlockParam() {
    return blockParam;
  }

  public LockType getLeaseLock() {
    return leaseLock;
  }

  public LockType getLpLock() {
    return lpLock;
  }

  public LockType getReplicaLock() {
    return replicaLock;
  }

  public LockType getCrLock() {
    return crLock;
  }

  public LockType getErLock() {
    return erLock;
  }

  public LockType getRucLock() {
    return rucLock;
  }

  public LockType getUrbLock() {
    return urbLock;
  }

  public LockType getPbLock() {
    return pbLock;
  }

  public LockType getInvLocks() {
    return invLocks;
  }

  public LockType getGenerationStampLock() {
    return generationStampLock;
  }

  public LockType getBlockIdCounterLock() {
    return blockIdCounterLock;
  }

  public LockType getLeaderLock() {
    return leaderLock;
  }

  public void addLockedINodes(INode inode, INodeLockType lock) {
    if (inode == null) {
      return;
    }

    //snapshot layer will prevent the read from going to db if it has already 
    //read that row. In a tx you can only read a row once. if you read again then
    //the snapshot layer will return the  cached value and the lock type will
    //remain the same as it was set when reading the row for the first time.
    //So if the lock for a indoe already exist in the hash map then
    //then there is no need to update the map
    if (!allLockedInodesInTx.containsKey(inode)) {
      allLockedInodesInTx.put(inode, lock);
      System.out.println("TX Locks inode id = " + inode.getId() + " has lock " + lock);
    }
  }

  public INodeLockType getLockedINodeLockType(INode inode) {
    return allLockedInodesInTx.get(inode);
  }

  public boolean isPreTxPathFullyResolved() {
    return preTxPathFullyResolved;
  }
}
