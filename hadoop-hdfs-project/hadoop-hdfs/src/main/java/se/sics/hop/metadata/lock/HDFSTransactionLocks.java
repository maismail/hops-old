package se.sics.hop.metadata.lock;

import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;
import java.util.HashMap;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
  private boolean ignoreLocalSubtreeLocks;
  private long namenodeId;
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
  //storage info
  private LockType storageInfoLock = null;
  //sidcounter
  private LockType sidCounterLock = null;
  //replicationIndex 
  private LockType replicationIndexLock = null;
  // individual inode lock
  private Integer inodeId = null;
  // outstanding quota updates
  private LockType quotaUpdatesLock = null;
  private Integer quotaUpdatesInodeId = null;
  private LockType quotaUpdatesLockSubtree = null;
  
  private long[] blocksParam = null;
  private int[] inodesParam = null;
  private Integer invdatanode = null;
  private Integer repldatanode = null;
  
  private LockType leaderTocken = null;
  private static Configuration conf;
  
  public HDFSTransactionLocks(){
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

  public HDFSTransactionLocks addINode(
      INodeResolveType resolveType,
      INodeLockType lock,
      boolean resolveLink,
      String[] param,
      boolean ignoreLocalSubtreeLocks,
      long namenodeId) {
    this.inodeLock = lock;
    this.inodeResolveType = resolveType;
    this.inodeParam = param;
    this.resolveLink = resolveLink;
    this.ignoreLocalSubtreeLocks = ignoreLocalSubtreeLocks;
    this.namenodeId = namenodeId;
    return this;
  }

  public HDFSTransactionLocks addINode(
      INodeResolveType resolveType,
      INodeLockType lock,
      boolean resolveLink,
      String[] param) {
    return addINode(resolveType, lock, resolveLink, param, false, 0);
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

  public HDFSTransactionLocks addIndividualInode(INodeLockType lock, Integer inodeId) {
    this.inodeLock = lock;
    this.inodeId = inodeId;
    return this;
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

  public HDFSTransactionLocks addReplicationIndex(LockType lock) {
    this.replicationIndexLock = lock;
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

  public HDFSTransactionLocks addLeaderTocken(LockType lock) {
    this.leaderTocken = lock;
    return this;
  }

  public HDFSTransactionLocks addQuotaUpdates(Integer inodeId) {
    this.quotaUpdatesLock = LockType.READ_COMMITTED;
    this.quotaUpdatesInodeId = inodeId;
    return this;
  }

  public HDFSTransactionLocks addQuotaUpdateOnSubtree() {
    this.quotaUpdatesLockSubtree = LockType.READ_COMMITTED;
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
  
  public LockType getReplicationIndexLock(){
    return replicationIndexLock;
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
  
  public LockType getLeaderLock() {
    return leaderLock;
  }

  public LockType getStorageInfo() {
    return storageInfoLock;
  }

  public LockType getSIdCounter(){
    return sidCounterLock;
  }
  
  public LockType getMaxNNID(){
    return leaderTocken;
  }
  
  public INode[] getInodeResult() {
    return inodeResult;
  }

  public boolean getIgnoreLocalSubtreeLocks() {
    return ignoreLocalSubtreeLocks;
  }

  public long getNamenodeId() {
    return namenodeId;
  }

  public LinkedList<INode> getPreTxResolvedInodes() {
    return preTxResolvedInodes;
  }

  public Integer getInodeId() {
    return inodeId;
  }

  public LockType getQuotaUpdatesLock() {
    return quotaUpdatesLock;
  }

  public LockType getQuotaUpdatesLockSubtree() {
    return quotaUpdatesLockSubtree;
  }

  public Integer getQuotaUpdatesInodeId() {
    return quotaUpdatesInodeId;
  }

  public void addLockedINodes(INode inode, INodeLockType lock) {
    if (inode == null) {
      return;
    }
    boolean insert = true;
    if(allLockedInodesInTx.containsKey(inode)){
      if(allLockedInodesInTx.get(inode).gt(lock)){
        insert = false;
      }
    }
    if(insert){
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
  
  public HDFSTransactionLocks addBlocks(long[] blocks, int[] inodes) {
    this.inodesParam = inodes;
    return addBlocks(blocks);
  }
   
  public long[] getBlocksParam() {
    return blocksParam;
  }

  public int[] getInodesParam(){
    return inodesParam;
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
  public static void setConfiguration(final Configuration config){
    conf = config;
  }
  public INodeLockType getPrecedingPathLockType(){
    String val = conf.get(DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE, DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE_DEFAULT);
    if(val.compareToIgnoreCase("READ")==0){
      return INodeLockType.READ;
    }
    else if(val.compareToIgnoreCase("READ_COMMITTED")==0){
      return INodeLockType.READ_COMMITTED;
    }else{
      throw new IllegalStateException("Critical Parameter is not defined. Set "+DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE);
    }
  }
}
