package se.sics.hop.metadata.lock;

import se.sics.hop.transaction.lock.TransactionLocks;
import java.util.HashMap;
import java.util.LinkedList;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeResolveType;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HDFSTransactionLocks implements TransactionLocks{

  //inode
  private INodeLockType inodeLock = null;
  private INodeResolveType inodeResolveType = null;
  private boolean resolveLink = true; // the file is a symlink should it resolve it?
  private String[] inodeParam = null;
  private INode[] inodeResult = null;
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
  // Leader
  private LockType leaderLock = null;
  // block token key
  private LockType blockKeyLock = null;
  // block generation stamp
  private LockType generationStampLock = null;
  //block id counter 
  private LockType blockIdCounterLock = null;
  //storage info
  private LockType storageInfoLock = null;
  //indode id counter
  private LockType inodeIDCounterLock = null;
  
  HDFSTransactionLocks() {
  }

  HDFSTransactionLocks(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    this.preTxResolvedInodes = resolvedInodes;
    this.preTxPathFullyResolved = preTxPathFullyResolved;
  }

  public String[] getInodeParam() {
    return inodeParam;
  }

  public boolean isResolveLink() {
    return resolveLink;
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

  public HDFSTransactionLocks addINode(INodeResolveType resolveType,
          INodeLockType lock, boolean resolveLink, String[] param) {
    this.inodeLock = lock;
    this.inodeResolveType = resolveType;
    this.inodeParam = param;
    this.resolveLink = resolveLink;
    return this;
  }

  public HDFSTransactionLocks addINode(INodeResolveType resolveType,
          INodeLockType lock, String[] param) {
    return addINode(resolveType, lock, true, param);
  }

  public HDFSTransactionLocks addINode(INodeLockType lock) {
    addINode(null, lock, null);
    return this;
  }

  public HDFSTransactionLocks addINode(INodeResolveType resolveType, INodeLockType lock) {
    return addINode(resolveType, lock, true, null);
  }

  public HDFSTransactionLocks addBlock(LockType lock, Long param) {
    this.blockLock = lock;
    this.blockParam = param;
    return this;
  }

  public HDFSTransactionLocks addBlock(LockType lock) {
    addBlock(lock, null);
    return this;
  }

  public HDFSTransactionLocks addLease(LockType lock, String param) {
    this.leaseLock = lock;
    this.leaseParam = param;
    return this;
  }

  public HDFSTransactionLocks addLease(LockType lock) {
    addLease(lock, null);
    return this;
  }

  public HDFSTransactionLocks addCorrupt(LockType lock) {
    this.crLock = lock;
    return this;
  }

  public HDFSTransactionLocks addExcess(LockType lock) {
    this.erLock = lock;
    return this;
  }

  public HDFSTransactionLocks addReplicaUc(LockType lock) {
    this.rucLock = lock;
    return this;
  }

  public HDFSTransactionLocks addReplica(LockType lock) {
    this.replicaLock = lock;
    return this;
  }

  public HDFSTransactionLocks addNameNodeLease(LockType lock) {
    this.nnLeaseLock = lock;
    return this;
  }

  public HDFSTransactionLocks addLeasePath(LockType lock) {
    this.lpLock = lock;
    return this;
  }

  public HDFSTransactionLocks addUnderReplicatedBlock(LockType lock) {
    this.urbLock = lock;
    return this;
  }

  public HDFSTransactionLocks addGenerationStamp(LockType lock) {
    this.generationStampLock = lock;
    return this;
  }

  public HDFSTransactionLocks addBlockIdCounter(LockType lock) {
    this.blockIdCounterLock = lock;
    return this;
  }

  public HDFSTransactionLocks addInodeIDCounterLock(LockType inodeIDCounterLock) {
    this.inodeIDCounterLock = inodeIDCounterLock;
    return this;
  }

  public HDFSTransactionLocks addBlockKey(LockType lock) {
    blockKeyLock = lock;
    return this;
  }

  public HDFSTransactionLocks addLeaderLock(LockType lock, long... ids) {
    this.leaderLock = lock;
    return this;
  }

  public HDFSTransactionLocks addInvalidatedBlock(LockType lock) {
    this.invLocks = lock;
    return this;
  }

  public HDFSTransactionLocks addPendingBlock(LockType lock) {
    this.pbLock = lock;
    return this;
  }

  public HDFSTransactionLocks addStorageInfo(LockType lock) {
    this.storageInfoLock = lock;
    return this;
  }

  public INodeLockType getInodeLock() {
    return inodeLock;
  }

  public INodeResolveType getInodeResolveType() {
    return inodeResolveType;
  }

  public LockType getBlockLock() {
    return LockType.READ_COMMITTED;
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
    return LockType.READ_COMMITTED;
  }

  public LockType getCrLock() {
    return LockType.READ_COMMITTED;
  }

  public LockType getErLock() {
    return LockType.READ_COMMITTED;
  }

  public LockType getRucLock() {
    return LockType.READ_COMMITTED;
  }

  public LockType getUrbLock() {
    return LockType.READ_COMMITTED;
  }

  public LockType getPbLock() {
    return LockType.READ_COMMITTED;
  }

  public LockType getInvLocks() {
    return LockType.READ_COMMITTED;
  }

  public LockType getGenerationStampLock() {
    return generationStampLock;
  }

  public LockType getBlockIdCounterLock() {
     return blockIdCounterLock;
  }

  public LockType getInodeIDCounterLock() {
    return inodeIDCounterLock;
  }

  public LockType getLeaderLock() {
    return leaderLock;
  }

  public LockType getStorageInfo() {
    return storageInfoLock;
  }

  public INode[] getInodeResult() {
    return inodeResult;
  }

  public LinkedList<INode> getPreTxResolvedInodes() {
    return preTxResolvedInodes;
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
    }
  }

  public INodeLockType getLockedINodeLockType(INode inode) {
    return allLockedInodesInTx.get(inode);
  }

  public boolean isPreTxPathFullyResolved() {
    return preTxPathFullyResolved;
  }
}
