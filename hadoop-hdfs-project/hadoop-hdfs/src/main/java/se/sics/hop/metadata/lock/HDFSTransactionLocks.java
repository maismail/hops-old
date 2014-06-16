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
  private Long blockID = null; //block id
  private Integer  blockInodeId = null;
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
  private boolean  urbLockFindAll = false;
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
  private int expectedMaxNumberofINodeIds = 0;
  //sidcounter
  private LockType sidCounterLock = null;
  
  private long[] blocksParam = null;
  private Integer invdatanode = null;
  private Integer repldatanode = null;
  
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

  public HDFSTransactionLocks addBlock(Long param, Integer blockInodeId) {
    this.blockLock = LockType.READ_COMMITTED;
    this.blockID = param;
    this.blockInodeId = blockInodeId;
    return this;
  }

  public HDFSTransactionLocks addBlock() {
    addBlock(null,null);
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

  public HDFSTransactionLocks addCorrupt() {
    this.crLock = LockType.READ_COMMITTED;
    return this;
  }

  public HDFSTransactionLocks addExcess() {
    this.erLock = LockType.READ_COMMITTED;
    return this;
  }

  public HDFSTransactionLocks addReplicaUc() {
    this.rucLock = LockType.READ_COMMITTED;
    return this;
  }

  public HDFSTransactionLocks addReplica() {
    this.replicaLock = LockType.READ_COMMITTED;
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

  public HDFSTransactionLocks addUnderReplicatedBlock() {
    this.urbLock = LockType.READ_COMMITTED;
    return this;
  }
  
  public HDFSTransactionLocks addUnderReplicatedBlockFindAll() {
    this.urbLock = LockType.READ_COMMITTED;
    this.urbLockFindAll = true;
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

  public HDFSTransactionLocks addInodeIDCounterLock(LockType inodeIDCounterLock, int expectedMaxNumberofInodeIds) {
    this.inodeIDCounterLock = inodeIDCounterLock;
    this.expectedMaxNumberofINodeIds = expectedMaxNumberofInodeIds;
    return this;
  }

  public HDFSTransactionLocks addInodeIDCounterLock(LockType inodeIDCounterLock) {
    return addInodeIDCounterLock(inodeIDCounterLock, 1);
  }
   
  public HDFSTransactionLocks addBlockKey(LockType lock) {
    blockKeyLock = lock;
    return this;
  }

  public HDFSTransactionLocks addLeaderLock(LockType lock, long... ids) {
    this.leaderLock = lock;
    return this;
  }

  public HDFSTransactionLocks addInvalidatedBlock() {
    this.invLocks = LockType.READ_COMMITTED;
    return this;
  }

  public HDFSTransactionLocks addPendingBlock() {
    this.pbLock = LockType.READ_COMMITTED;
    return this;
  }

  public HDFSTransactionLocks addStorageInfo(LockType lock) {
    this.storageInfoLock = lock;
    return this;
  }
  
  public HDFSTransactionLocks addSIdCounter(LockType lock) {
    this.sidCounterLock = lock;
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

  public Long getBlockID() {
    return blockID;
  }

  public Integer getBlockInodeId() {
    return blockInodeId;
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

  public boolean isUrbLockFindAll() {
    return urbLockFindAll;
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

  public LockType getInodeIDCounterLock() {
    return inodeIDCounterLock;
  }
  
  public int getExpectedMaxNumberOfINodeIds(){
    return expectedMaxNumberofINodeIds;
  }
  
  public LockType getLeaderLock() {
    return leaderLock;
  }

  public LockType getStorageInfo() {
    return storageInfoLock;
  }

  public LockType getSIdCounter(){
    return sidCounterLock;
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
  
  public HDFSTransactionLocks addBlocks(long[] blocks) {
    this.blockLock = LockType.READ_COMMITTED;
    this.blocksParam = blocks;
    return this;
  }

  public long[] getBlocksParam() {
    return blocksParam;
  }

  public HDFSTransactionLocks addInvalidatedBlocks(int datanode) {
    this.invLocks = LockType.READ_COMMITTED;
    this.invdatanode = datanode;
    return this;
  }

  public HDFSTransactionLocks addReplicas(int datanode) {
    this.replicaLock = LockType.READ_COMMITTED;
    this.repldatanode = datanode;
    return this;
  }

  public Integer getInvalidatedBlocksDatanode() {
    return invdatanode;
  }

  public Integer getReplicasDatanode() {
    return repldatanode;
  }
}
