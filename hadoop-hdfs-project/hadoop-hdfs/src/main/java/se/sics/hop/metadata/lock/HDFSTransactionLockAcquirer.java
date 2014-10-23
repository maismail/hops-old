package se.sics.hop.metadata.lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import se.sics.hop.common.GlobalThreadPool;
import se.sics.hop.exception.AcquireLockInterruptedException;
import se.sics.hop.exception.INodeResolveException;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.LeaderElection;
import se.sics.hop.metadata.context.BlockPK;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.metadata.hdfs.entity.hop.*;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.lock.ParallelReadThread;
import se.sics.hop.transaction.lock.TransactionLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeResolveType;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import se.sics.hop.log.NDCWrapper;
import se.sics.hop.memcache.PathMemcache;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Salman <salman@sics.se>
 */
public class HDFSTransactionLockAcquirer extends TransactionLockAcquirer{

  private final  Log LOG = LogFactory.getLog(HDFSTransactionLockAcquirer.class);
  private final HDFSTransactionLocks locks;
  private LinkedList<LinkedList<INode>> allResolvedINodes = new LinkedList<LinkedList<INode>>(); //linked lists are important. we need to perserv insertion order
  private LinkedList<Lease> leaseResults = new LinkedList<Lease>();
  private LinkedList<BlockInfo> blockResults = new LinkedList<BlockInfo>();
  private boolean terminateAsyncThread = false;
  private static Configuration conf;
  private static Collection<ActiveNamenode> activeNamenodes;
  private static boolean isAsyncReadingEnabled  = false;

  public HDFSTransactionLockAcquirer() {
    this.locks = new HDFSTransactionLocks();
  }

  public HDFSTransactionLockAcquirer(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    this.locks = new HDFSTransactionLocks(resolvedInodes, preTxPathFullyResolved);
  }

  public static void setConfiguration(Configuration c) {
    conf = c;
    HDFSTransactionLocks.setConfiguration(conf);
  }

  public HDFSTransactionLocks getLocks() {
    return this.locks;
  }

  @Override
  public TransactionLocks acquire()
      throws PersistanceException, UnresolvedPathException, ExecutionException, SubtreeLockedException { //start taking locks from inodes
    // acuires lock in order
    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      
      setPartitioningKey(PathMemcache.getInstance().getPartitionKey(locks.getInodeParam()[0]));
    
      acquireInodeLocks(locks.getInodeParam());
    } else if (locks.getInodeLock() != null && locks.getInodeId() != null) {
      acquireIndividualInodeLock();
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockID() != null) {
        throw new StorageException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }
      blockResults.addAll(acquireBlockLock());
    }

    List<Future> futures = startNonLockingAsyncReadThreads();
    acquireLeaseAndLpathLockNormal();
    acquireLocksOnVariablesTable();
    acquireOutstandingQuotaUpdates();
    checkTerminationOfAsyncThreads(futures);
    return locks;
  }

  //just read all related Inode data withou taking any locks on the path
  public HDFSTransactionLocks acquire(INodeIdentifier inodeIdentifer) throws PersistanceException, UnresolvedPathException, ExecutionException {
    INode inode = null;
    if (inode == null && inodeIdentifer != null) {
      setPartitioningKey(inodeIdentifer.getInodeId());
      // dangling block
      // take lock on the indeId basically bring null in the cache
      if (inodeIdentifer.getName() != null && inodeIdentifer.getPid() != null) {
        inode = pkINodeLookUpByNameAndPid(locks.getInodeLock(), inodeIdentifer.getName(), inodeIdentifer.getPid(), locks);
        if (inode == null) {
          //there's no inode for this specific name,parentid which means this file is deleted
          //so fallback to the scan to update the inodecontext cache
          throw new StorageException("Abort the transaction because INode doesn't exists for " + inodeIdentifer);
        }
      } else if (inodeIdentifer.getInodeId() != null) {
        inode = iNodeScanLookUpByID(locks.getInodeLock(), inodeIdentifer.getInodeId(), locks);
      } else {
        throw new StorageException("INodeIdentifier objec is not properly initialized ");
      }
    }


    if (inode != null) {
      LinkedList<INode> resolvedINodeForBlk = new LinkedList<INode>();
      resolvedINodeForBlk.add(inode);
      allResolvedINodes.add(resolvedINodeForBlk);

      List<BlockInfo> allBlks = (List<BlockInfo>) acquireLockList(locks.getBlockLock(), BlockInfo.Finder.ByInodeId, inode.getId());
      blockResults.addAll(allBlks);

      // if the allBlks does not contain the locks.blocksParam block then
      // re-read it to bring null in the cache. the block was there in the pre-tx phase
      // but was deleted before the locks were acquired
      boolean found = false;
      if (locks.getBlockID() != null) {
        for (BlockInfo blk : allBlks) {
          if (blk.getBlockId() == locks.getBlockID()) {
            found = true;
            break;
          }
        }

        if (!found) {
          acquireLock(LockType.READ_COMMITTED, BlockInfo.Finder.ById, locks.getBlockID(), locks.getBlockInodeId());
          // we need to bring null for the other tables too. so put a dummy obj in the blocksResults list
          BlockInfo blk = new BlockInfo();
          if (inode != null) {
            blk.setINodeIdNoPersistance(inode.getId());
          }
          blk.setBlockIdNoPersistance(locks.getBlockID());

          blockResults.add(blk);
        }
      }

      // sort the blocks. it is important as the ndb returns the blocks in random order and two
      // txs trying to take locks on the blocks of a file will end up in dead lock 
      Collections.sort((List<BlockInfo>) blockResults, BlockInfo.Order.ByBlockId);
    }

    if (blockResults.isEmpty() && locks.getBlockID() != null) {
      BlockInfo block = acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, locks.getBlockID(), locks.getBlockInodeId());
      if (block != null) {
        blockResults.add(block);
      }
    }

    // read-committed block is the same as block found by inode-file so everything is fine and continue the rest.
    List<Future> futures = startNonLockingAsyncReadThreads();
    acquireLeaseAndLpathLockNormal();
    acquireLocksOnVariablesTable();
    checkTerminationOfAsyncThreads(futures);
    return locks;
  }
  /**
   * This method acquires lockk on the inode starting with a block-id. The
   * lock-types should be set before using add* methods. Otherwise, no lock
   * would be acquired.
   *
   * @throws PersistanceException
   */
  public HDFSTransactionLocks acquireByBlock(INodeIdentifier inodeIdentifer) throws PersistanceException, UnresolvedPathException, ExecutionException {
    boolean isPartKeySet = false;
    INode inode = null;
    if (locks.getInodeResolveType() == INodeResolveType.PATH) {
      checkPathIsResolved();
      if (!locks.getPreTxResolvedInodes().isEmpty()) {
        if(!isPartKeySet){
          isPartKeySet = true;
          setPartitioningKey(locks.getPreTxResolvedInodes().peekLast().getId());
        }
        inode = takeLocksFromRootToLeaf(locks.getPreTxResolvedInodes(), locks.getInodeLock());
        allResolvedINodes.add(locks.getPreTxResolvedInodes());
      }
    } 
    
    if (inode == null && inodeIdentifer != null) {
      if(!isPartKeySet){
          setPartitioningKey(inodeIdentifer.getInodeId());
        }
      
      if(inodeIdentifer.getName()!=null&& inodeIdentifer.getPid()!=null){
          inode = pkINodeLookUpByNameAndPid(locks.getInodeLock(),inodeIdentifer.getName(), inodeIdentifer.getPid(),locks);
          if(inode == null){
            //there's no inode for this specific name,parentid which means this file is deleted
            //so fallback to the scan to update the inodecontext cache
            throw new StorageException("Abort the transaction because INode doesn't exists for " + inodeIdentifer);
          }
      }else if(inodeIdentifer.getInodeId() != null ){
          inode = iNodeScanLookUpByID(locks.getInodeLock(), inodeIdentifer.getInodeId(), locks);
      }else {
          throw new StorageException("INodeIdentifier objec is not properly initialized ");
      }
    }
    

    if (inode != null) {
      LinkedList<INode> resolvedINodeForBlk = new LinkedList<INode>();
      resolvedINodeForBlk.add(inode);
      allResolvedINodes.add(resolvedINodeForBlk);

      List<BlockInfo> allBlks =  (List<BlockInfo>)acquireLockList(locks.getBlockLock(), BlockInfo.Finder.ByInodeId, inode.getId());
      blockResults.addAll(allBlks);
      
      // if the allBlks does not contain the locks.blocksParam block then
      // re-read it to bring null in the cache. the block was there in the pre-tx phase
      // but was deleted before the locks were acquired
      boolean found = false;
      if (locks.getBlockID() != null) {
        for (BlockInfo blk : allBlks) {
          if (blk.getBlockId() == locks.getBlockID()) {
            found = true;
            break;
          }
        }

        if (!found) {
          acquireLock(LockType.READ_COMMITTED, BlockInfo.Finder.ById, locks.getBlockID(), locks.getBlockInodeId());
          // we need to bring null for the other tables too. so put a dummy obj in the blocksResults list
          BlockInfo blk = new BlockInfo();
          if (inode != null) {
            blk.setINodeIdNoPersistance(inode.getId());
          }
          blk.setBlockIdNoPersistance(locks.getBlockID());
          
          blockResults.add(blk);
        }
      }
      
      // sort the blocks. it is important as the ndb returns the blocks in random order and two
      // txs trying to take locks on the blocks of a file will end up in dead lock 
      Collections.sort((List<BlockInfo>) blockResults, BlockInfo.Order.ByBlockId);
    }

    if (blockResults.isEmpty()) {
      BlockInfo block = acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, locks.getBlockID(),locks.getBlockInodeId());
      if (block != null) {
        blockResults.add(block);
      }
    }

    // read-committed block is the same as block found by inode-file so everything is fine and continue the rest.
    List<Future> futures = startNonLockingAsyncReadThreads();
    acquireLeaseAndLpathLockNormal();
    acquireLocksOnVariablesTable();
    checkTerminationOfAsyncThreads(futures);
    return locks;
  }

  public HDFSTransactionLocks acquireByLease(SortedSet<String> sortedPaths)
      throws PersistanceException, UnresolvedPathException, ExecutionException, SubtreeLockedException {
    if (locks.getLeaseParam() == null) {
      return locks;
    }
    
    setPartitioningKey(null);
      
    acquireInodeLocks(sortedPaths.toArray(new String[sortedPaths.size()]));

    blockResults.addAll(acquireBlockLock());
    
    List<Future> futures = startNonLockingAsyncReadThreads();

    Lease nnLease = acquireNameNodeLease(); // NameNode lease is always acquired first.
    if (nnLease != null) {
      leaseResults.add(nnLease);
    }
    Lease lease = acquireLock(locks.getLeaseLock(), Lease.Finder.ByPKey, locks.getLeaseParam());
    if (lease == null) {
      return locks; // Lease does not exist anymore.
    }
    leaseResults.add(lease);

    List<HopLeasePath> lpResults = acquireLeasePathsLock();
    if (lpResults.size() > sortedPaths.size()) {
      return locks; // TODO: It should retry again, cause there are new lease-paths for this lease which we have not acquired their inodes locks.
    }
    acquireLocksOnVariablesTable();
    checkTerminationOfAsyncThreads(futures);
    return locks;
  }

  /**
   * Acquires lock on lease path and lease having leasepath. This is used by the
   * test cases.
   *
   * @param leasePath
   */
  public HDFSTransactionLocks acquireByLeasePath(String leasePath, TransactionLockTypes.LockType leasePathLock, TransactionLockTypes.LockType leaseLock) throws PersistanceException {
    HopLeasePath lp = acquireLock(leasePathLock, HopLeasePath.Finder.ByPKey, leasePath);
    if (lp != null) {
      acquireLock(leaseLock, Lease.Finder.ByHolderId, lp.getHolderId());
    }
    return locks;
  }

  public HDFSTransactionLocks acquireForRename()
      throws PersistanceException, UnresolvedPathException, ExecutionException, SubtreeLockedException {
    return acquireForRename(false);
  }

  public HDFSTransactionLocks acquireForRename(boolean allowExistingDir)
      throws PersistanceException, UnresolvedPathException, ExecutionException, SubtreeLockedException {
    byte[][] srcComponents = INode.getPathComponents(locks.getInodeParam()[0]);
    byte[][] dstComponents = INode.getPathComponents(locks.getInodeParam()[1]);

    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      //[S] consider src = /a/b/c and dst = /d
      //during the acquire lock of src write locks will be acquired on parent of c and c
      //during the acquire lock of dst write lock on the root will be acquired but the snapshot 
      //layer will not let the request go to the db as it has already cached the root inode
      //one simple solution is that to acquire lock on the short path first
      setPartitioningKey(PathMemcache.getInstance().getPartitionKey(locks.getInodeParam()[0]));
      if (srcComponents.length <= dstComponents.length) {
        acquireInodeLocks(locks.getInodeParam()[0]);
        acquireInodeLocks(locks.getInodeParam()[1]);
      } else {
        acquireInodeLocks(locks.getInodeParam()[1]);
        acquireInodeLocks(locks.getInodeParam()[0]);
      }

      if (allowExistingDir) // In deprecated rename, it allows to move a dir to an existing destination.
      {
        LinkedList<INode> dstINodes = acquireInodeLockByPath(locks, locks.getInodeParam()[1]); // reads from snapshot.  
        if (dstINodes.size() == dstComponents.length && dstINodes.getLast().isDirectory()) {
          //the dst exist and is a directory.
          INode existingInode = pkINodeLookUpByNameAndPid(
                  locks.getInodeLock(),
                  DFSUtil.bytes2String(srcComponents[srcComponents.length - 1]),
                  dstINodes.getLast().getId(), locks);
        }
      }
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockID() != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }
      blockResults.addAll(acquireBlockLock());
    }

    List<Future> futures = startNonLockingAsyncReadThreads();
    acquireLeaseAndLpathLockNormal();
    acquireLocksOnVariablesTable();
    acquireOutstandingQuotaUpdates();
    checkTerminationOfAsyncThreads(futures);
    return locks;
  }

  public TransactionLocks acquireBatch() throws PersistanceException {
    int[] inodeIds = locks.getInodesParam();
    if (locks.getBlockLock() != null && locks.getBlocksParam() != null) {
      if (inodeIds == null) {
        inodeIds = INodeUtil.resolveINodesFromBlockIds(locks.getBlocksParam());
      }
      acquireLockList(locks.getBlockLock(), BlockInfo.Finder.ByIds, locks.getBlocksParam(), inodeIds);
    }
    if (locks.getInvLocks() != null && locks.getBlocksParam() != null && locks.getInvalidatedBlocksDatanode() != null && inodeIds != null) {
      acquireLockList(locks.getInvLocks(), HopInvalidatedBlock.Finder.ByPKS, locks.getBlocksParam(), inodeIds, locks.getInvalidatedBlocksDatanode());
    }

    if (locks.getReplicaLock() != null && locks.getBlocksParam() != null && locks.getReplicasDatanode() != null && inodeIds != null) {
      acquireLockList(locks.getReplicaLock(), HopIndexedReplica.Finder.ByPKS, locks.getBlocksParam(), inodeIds, locks.getReplicasDatanode());
    }
    return locks;
  }

  private LinkedList<Lease> acquireLeaseLock() throws PersistanceException {
    checkStringParam(locks.getLeaseParam());
    SortedSet<String> holders = new TreeSet<String>();
    if (locks.getLeaseParam() != null) {
      holders.add((String) locks.getLeaseParam());
    }

    for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
      for (INode f : resolvedINodes) {
        if (f instanceof INodeFileUnderConstruction) {
          holders.add(((INodeFileUnderConstruction) f).getClientName());
        }
      }
    }

    LinkedList<Lease> leases = new LinkedList<Lease>();
    for (String h : holders) {
      Lease lease = acquireLock(locks.getLeaseLock(), Lease.Finder.ByPKey, h);
      if (lease != null) {
        leases.add(lease);
      }
    }

    return leases;
  }

  private void checkStringParam(Object param) {
    if (param != null && !(param instanceof String)) {
      throw new IllegalArgumentException("Param is expected to be a String but is " + param.getClass().getName());
    }
  }

  private List<HopLeasePath> acquireLeasePathsLock() throws PersistanceException {
    List<HopLeasePath> lPaths = new LinkedList<HopLeasePath>();
    if (leaseResults != null) {
      for (Lease l : leaseResults) {
        Collection<HopLeasePath> result = acquireLockList(locks.getLpLock(), HopLeasePath.Finder.ByHolderId, l.getHolderID());
        if (!l.getHolder().equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) { // We don't need to keep the lps result for namenode-lease. 
          lPaths.addAll(result);
        }
      }
    }

    return lPaths;
  }

  private Future acquireBlockRelatedTableLocksASync(final ParallelReadParams parallelReadParams) throws PersistanceException {
    final String threadName = getTransactionName();
    final Long parentThreadId = Thread.currentThread().getId();
    ParallelReadThread pThread = new ParallelReadThread(Thread.currentThread().getId(), parallelReadParams) {
      @Override
       public void run() {
         try {
           if(isAsyncReadingEnabled){  
             NDCWrapper.push(threadName);
           }
           if(parallelReadParams.getInodeIds() != null && !parallelReadParams.getInodeIds().isEmpty() && parallelReadParams.getInodeFinder() != null ){
             for(HopINodeCandidatePK inodeParam : parallelReadParams.getInodeIds()){
               if (!terminateAsyncThread) {
                 concurrentAcquireLockList(LockType.READ_COMMITTED, parallelReadParams.getInodeFinder(), parentThreadId, inodeParam.getInodeId());
               }
             }
           }
           else if (parallelReadParams.getBlockIds() != null && !parallelReadParams.getBlockIds().isEmpty() && parallelReadParams.getBlockFinder() != null ){
             for(BlockPK blkParam : parallelReadParams.getBlockIds()){
               if (!terminateAsyncThread) {
                 if(parallelReadParams.isListBlockFinder){
                   concurrentAcquireLockList(LockType.READ_COMMITTED, parallelReadParams.blockFinder, parentThreadId, blkParam.id, blkParam.inodeId);
                 }else{
                   concurrentAcquireLock(LockType.READ_COMMITTED, parallelReadParams.blockFinder, parentThreadId, blkParam.id, blkParam.inodeId);
                 }
               }
             }
           }
           else if (parallelReadParams.getDefaultFinder()!=null && !terminateAsyncThread) {
             concurrentAcquireLockList(LockType.READ_COMMITTED, parallelReadParams.getDefaultFinder(), parentThreadId);
           }else{
              LOG.debug("Could not take locks for "+" "+ (parallelReadParams.getBlockFinder() != null?parallelReadParams.getBlockFinder().getClass().getName():"")
                            + " "+ (parallelReadParams.getInodeFinder()!= null?parallelReadParams.getInodeFinder().getClass().getName():"")
                            + " "+ (parallelReadParams.getDefaultFinder()!= null?parallelReadParams.getDefaultFinder().getClass().getName():""));
           }
         } catch (Exception ex) {
           exceptionList.add(ex); //after join all exceptions will be thrown
         }
         if(isAsyncReadingEnabled){
           NDCWrapper.pop();
           NDCWrapper.remove();
         }
       }
     };
    if(isAsyncReadingEnabled){
    Future future = GlobalThreadPool.getExecutorService().submit(pThread);
    return future;
    }else{
        pThread.run();
        return null;
    }
  }
  
  List<Exception> exceptionList = new ArrayList<Exception>();

  private  <T> Collection<T> concurrentAcquireLockList(LockType lock, FinderType<T> finder, Long parentThreadId, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return EntityManager.concurrentFindList(finder, parentThreadId);
    } else {
      return EntityManager.concurrentFindList(finder, parentThreadId, param);
    }
  }

  private  <T> T concurrentAcquireLock(LockType lock, FinderType<T> finder, Long parentThreadId, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return null;
    }
    return EntityManager.concurrentFind(finder, parentThreadId, param);
  }

  private LinkedList<INode> findImmediateChildren(INode lastINode) throws PersistanceException {
    LinkedList<INode> children = new LinkedList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        setINodeLockType(locks.getInodeLock());
        children.addAll(((INodeDirectory) lastINode).getChildren());
      }
    }
    return children;
  }

  private LinkedList<INode> findChildrenRecursively(INode lastINode) throws PersistanceException {
    LinkedList<INode> children = new LinkedList<INode>();
    LinkedList<INode> unCheckedDirs = new LinkedList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        unCheckedDirs.add(lastINode);
      }
    }

    // Find all the children in the sub-directories.
    while (!unCheckedDirs.isEmpty()) {
      INode next = unCheckedDirs.poll();
      if (next instanceof INodeDirectory) {
        setINodeLockType(locks.getInodeLock());
        List<INode> clist = ((INodeDirectory) next).getChildren();
        unCheckedDirs.addAll(clist);
        children.addAll(clist);
      } 
    }
    LOG.debug("Added "+children.size()+" childern.");
    return children;
  }

  private String buildPath(String path, int size) {
    StringBuilder builder = new StringBuilder();
    byte[][] components = INode.getPathComponents(path);

    for (int i = 0; i < Math.min(components.length, size); i++) {
      if (i == 0) {
        builder.append("/");
      } else {
        if (i != 1) {
          builder.append("/");
        }
        builder.append(DFSUtil.bytes2String(components[i]));
      }
    }

    return builder.toString();
  }

  private Lease acquireNameNodeLease() throws PersistanceException {
    if (locks.getNnLeaseLock() != null) {
      return acquireLock(locks.getNnLeaseLock(), Lease.Finder.ByPKey, HdfsServerConstants.NAMENODE_LEASE_HOLDER);
    }
    return null;
  }

  public HDFSTransactionLocks acquireLeaderLock() throws PersistanceException {
    if (locks.getLeaderLock() != null) {
      setPartitioningKeyForLeader();
      acquireLocksOnVariablesTable();
      acquireLockList(locks.getLeaderLock(), HopLeader.Finder.All);
    }
    return locks;
  }

  private void acquireLeaseAndLpathLockNormal() throws PersistanceException {
    if (locks.getLeaseLock() != null) {
      leaseResults.addAll(acquireLeaseLock());
    }

    if (locks.getLpLock() != null) {
      acquireLeasePathsLock();
    }
  }

  private void acquireLocksOnVariablesTable()throws PersistanceException {
    //variables table
    if (locks.getBlockKeyLock() != null) {
      acquireLock(locks.getBlockKeyLock(), HopVariable.Finder.BlockTokenKeys);
    }
    
    if (locks.getStorageInfo() != null) {
      acquireLock(locks.getStorageInfo(), HopVariable.Finder.StorageInfo);
    }
    
    if(locks.getReplicationIndexLock() != null){
       acquireLock(locks.getReplicationIndexLock(), HopVariable.Finder.ReplicationIndex);
    }
    
    if (locks.getSIdCounter() != null) {
      acquireLock(locks.getSIdCounter(), HopVariable.Finder.SIdCounter);
    }
    
    if (locks.getMaxNNID() != null) {
      acquireLock(locks.getMaxNNID(), HopVariable.Finder.MaxNNID);
    }
  }
  /**
   * Acquires lock on the lease, lease-path, replicas, excess, corrupt,
   * invalidated, under-replicated and pending blocks.
   *
   * @throws PersistanceException
   */
  private List<Future> acquireBlockRelatedInfoASync() throws PersistanceException, ExecutionException {
    // blocks related tables
    List<Future> futures = new ArrayList<Future>();
    if (locks.getReplicaLock() != null) {
      ParallelReadParams parallelReadParams = getBlockParameters(HopIndexedReplica.Finder.ByBlockId, true, HopIndexedReplica.Finder.ByINodeId, null);
      Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
    }

    if (locks.getCrLock() != null) {
      ParallelReadParams parallelReadParams = getBlockParameters(HopCorruptReplica.Finder.ByBlockId, true, HopCorruptReplica.Finder.ByINodeId, null);
      Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
    }

    if (locks.getErLock() != null) {
      ParallelReadParams parallelReadParams = getBlockParameters(HopExcessReplica.Finder.ByBlockId, true, HopExcessReplica.Finder.ByINodeId, null);
      Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
    }

    if (locks.getRucLock() != null) {
      ParallelReadParams parallelReadParams = getBlockParameters(ReplicaUnderConstruction.Finder.ByBlockId, true, ReplicaUnderConstruction.Finder.ByINodeId , null);
      Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
    }

    if (locks.getInvLocks() != null) {
      ParallelReadParams parallelReadParams = getBlockParameters(HopInvalidatedBlock.Finder.ByBlockId, true, HopInvalidatedBlock.Finder.ByINodeId, null);
      Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
    }

    if (locks.getUrbLock() != null) {
      if(locks.isUrbLockFindAll()){
        ParallelReadParams parallelReadParams = new ParallelReadParams(null, null, false, null, null,HopUnderReplicatedBlock.Finder.All );
        Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
      }else{
        ParallelReadParams parallelReadParams = getBlockParameters(HopUnderReplicatedBlock.Finder.ByBlockId, false, HopUnderReplicatedBlock.Finder.ByINodeId, null);
        Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
      }
    }

    if (locks.getPbLock() != null) {
      ParallelReadParams parallelReadParams = getBlockParameters(PendingBlockInfo.Finder.ByBlockId, false,PendingBlockInfo.Finder.ByInodeId,null);
      Future future = acquireBlockRelatedTableLocksASync(parallelReadParams);
      if(future != null){
        futures.add(future);
      }
    }
    
   return futures;
  }
  
  private void checkTerminationOfAsyncThreads(List<Future> futures) throws PersistanceException, ExecutionException {
    
    if(!isAsyncReadingEnabled){
       assert futures.size() == 0 : " Futures list should have been empty ";
       return;
    }
    
    
       InterruptedException intrException = null;
    try {
      for (int i = 0; i < futures.size(); i++) {
        Future f = futures.get(i);
        f.get();
      }
    } catch (InterruptedException e) {
      terminateAsyncThread = true;
      if(intrException == null){
        intrException = e;
      }
    }
    futures.clear();
    if(intrException != null){
      throw new AcquireLockInterruptedException(intrException);
    }
    
    if(exceptionList.size() > 0){
      for(int i = 0; i < exceptionList.size(); i++){
        Exception e = exceptionList.get(i);
        e.printStackTrace();
      }
      // throw first exception. Its better to throw the hardest of all exceptions
      // problem is which exception is the hardest. 
      Exception exception = exceptionList.get(0);
      exceptionList.clear();
      LOG.debug("Throwing the exception "+NDCWrapper.peek()+" - "+exception.getClass().getCanonicalName()+" Message: " + exception.getMessage());
      if(exception instanceof PersistanceException){
        throw (PersistanceException)exception;
      }else{
        exception.printStackTrace();
        throw new StorageException(NDCWrapper.peek()+" - "+exception.getClass().getCanonicalName()+" Message: " + exception.getMessage());
      }
    }
  }
   
  
  private ParallelReadParams getBlockParameters(FinderType blockFinder, boolean isListBlockFinder, FinderType inodeFinder, FinderType defaultFinder) {
    List<HopINodeCandidatePK> inodesParams = new ArrayList<HopINodeCandidatePK>();
    List<BlockPK> blocksParams = new ArrayList<BlockPK>();
    
    // first try to take locks based on inodes
    if (allResolvedINodes != null) {
      for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
        for (INode inode : resolvedINodes) {
          if (inode instanceof INodeFile || inode instanceof INodeFileUnderConstruction) {
            HopINodeCandidatePK param = new HopINodeCandidatePK(inode.getId());
            inodesParams.add(param);
          }
        }
      }
    }
    
    // if no inodes found then
    // try to take locks based on blcoks
//    if( inodesParams.isEmpty() ){
      if (blockResults != null && !blockResults.isEmpty()) {
        for (BlockInfo b : blockResults) {
          blocksParams.add(new BlockPK(b.getBlockId(), b.getInodeId()));
         // LOG.debug("Param blk "+b.getBlockId()+" paratKey "+b.getPartKey());
        }
      } else // if blockResults is null then we can safely bring null in to cache
      {
        if (locks.getBlockID() != null) {
          blocksParams.add(new BlockPK(locks.getBlockID(),locks.getBlockInodeId()));
        }
      }      
//    }
    

    return new ParallelReadParams(blocksParams, blockFinder, isListBlockFinder, inodesParams, inodeFinder, defaultFinder);
  }

  private void acquireInodeLocks(String... params)
      throws UnresolvedPathException, PersistanceException, SubtreeLockedException {
    switch (locks.getInodeResolveType()) {
      case PATH: // Only use memcached for this case.
      case PATH_AND_IMMEDIATE_CHILDREN: // Memcached not applicable for delete of a dir (and its children)
      case PATH_AND_ALL_CHILDREN_RECURESIVELY:
        for (int i = 0; i < params.length; i++) {
          LinkedList<INode> resolvedInodes = acquireInodeLockByPath(locks, params[i]);
          if (resolvedInodes.size() > 0) {
            INode lastINode = resolvedInodes.peekLast();
            acquireQuotaUpdate(lastINode);
            if (locks.getInodeResolveType() == INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
              List<INode> children = findImmediateChildren(lastINode);
              resolvedInodes.addAll(children);
              acquireQuotaUpdate(children);
            } else if (locks.getInodeResolveType() == INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY) {
              List<INode> children = findChildrenRecursively(lastINode);
              resolvedInodes.addAll(children);
              acquireQuotaUpdate(children);
            }
          }
          allResolvedINodes.add(resolvedInodes);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + locks.getInodeLock().name());
    }
  }

  private void acquireQuotaUpdate(List<INode> nodes) throws PersistanceException {
    for (INode node : nodes) {
      acquireQuotaUpdate(node);
    }
  }

  private void acquireQuotaUpdate(INode node) throws PersistanceException {
    if (getLocks().getQuotaUpdatesLockSubtree() != null) {
      EntityManager.findList(QuotaUpdate.Finder.ByInodeId, node.getId());
    }
  }

  private void checkPathIsResolved() throws INodeResolveException {
    if (locks.getPreTxResolvedInodes() == null) {
      throw new INodeResolveException(String.format(
              "Requires to have inode-id(s) in order to do this operation. "
              + "ResolvedInodes is null."));
    }
  }

  private LinkedList<BlockInfo> acquireBlockLock() throws PersistanceException {

    LinkedList<BlockInfo> blocks = new LinkedList<BlockInfo>();

    if (locks.getBlockID() != null) {

      BlockInfo result = acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, (Long) locks.getBlockID(), locks.getBlockInodeId());
      if (result != null) {
        blocks.add(result);
      }
    } else {
      for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
        for (INode inode : resolvedINodes) {
          if (inode instanceof INodeFile) {
            blocks.addAll(acquireLockList(locks.getBlockLock(), BlockInfo.Finder.ByInodeId, inode.getId()));
          }
        }
      }
    }

    // sort the blocks. it is important as the ndb returns the blocks in random order and two
    // txs trying to take locks on the blocks of a file will end up in dead lock 
    Collections.sort(blocks, BlockInfo.Order.ByBlockId);

    return blocks;
  }

  private void acquireOutstandingQuotaUpdates() throws PersistanceException {
    Integer inodeId = getLocks().getQuotaUpdatesInodeId();
    if (getLocks().getQuotaUpdatesLock() != null && inodeId != null) {
      acquireLockList(getLocks().getQuotaUpdatesLock(), QuotaUpdate.Finder.ByInodeId, inodeId);
    }
  }

  private INode takeLocksFromRootToLeaf(LinkedList<INode> inodes, INodeLockType inodeLock) throws PersistanceException {
    LOG.debug("Taking lock on preresolved path. Path Components are " + inodes.size());
    StringBuilder msg = new StringBuilder();
    msg.append("Took Lock on the entire path ");
    INode lockedLeafINode = null;
    
    for (int i = 0; i < inodes.size(); i++) {
      if (i == (inodes.size() - 1)) // take specified lock
      {
        lockedLeafINode = pkINodeLookUpByNameAndPid(inodeLock, inodes.get(i).getLocalName(),inodes.get(i).getParentId(), locks);
      } else // take read commited lock
      {
        lockedLeafINode = pkINodeLookUpByNameAndPid(INodeLockType.READ_COMMITTED, inodes.get(i).getLocalName(), inodes.get(i).getParentId(), locks);
      }

      if(lockedLeafINode == null){
        throw new StorageException("Abort the transaction because INode doesn't exists for " + inodes.get(i).getLocalName()+inodes.get(i).getParentId());
      }
      else if (!lockedLeafINode.getLocalName().equals("")) {
        msg.append("/");
        msg.append(lockedLeafINode.getLocalName());
      }
      
    }
    LOG.debug(msg.toString());
    return lockedLeafINode;
  }

  //TransacationLockAcquirer Code
  private  <T> Collection<T> acquireLockList(LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return EntityManager.findList(finder);
    } else {
      return EntityManager.findList(finder, param);
    }
  }

  private  <T> T acquireLock(LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return null;
    }
    return EntityManager.find(finder, param);
  }

  private  LinkedList<INode> acquireLockOnRestOfPath(INodeLockType lock, INode baseInode,
          String fullPath, String prefix, boolean resolveLink) throws PersistanceException, UnresolvedPathException {
    LinkedList<INode> resolved = new LinkedList<INode>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    int[] count = new int[]{prefixComps.length - 1};
    boolean lastComp;
    INode[] curInode = new INode[]{baseInode};
    while (count[0] < fullComps.length && curInode[0] != null) {
      setINodeLockType(lock);
      lastComp = INodeUtil.getNextChild(
              curInode,
              fullComps,
              count,
              resolveLink,
              true);
      if (curInode[0] != null) {
         locks.addLockedINodes(curInode[0], lock);
         resolved.add(curInode[0]);
      }
      if (lastComp) {
        break;
      }
    }

    return resolved;
  }

  private  LinkedList<INode> acquireInodeLockByPath(HDFSTransactionLocks locks, String path)
      throws UnresolvedPathException, PersistanceException, SubtreeLockedException {
    LinkedList<INode> resolvedInodes = new LinkedList<INode>();

    if (path == null) {
      return resolvedInodes;
    }

    byte[][] components = INode.getPathComponents(path);
    INode[] curNode = new INode[1];

    int[] count = new int[]{0};
    boolean lastComp = (count[0] == components.length - 1);
    if (lastComp) { // if root is the last directory, we should acquire the write lock over the root
      resolvedInodes.add(acquireLockOnRoot(locks.getInodeLock(), locks));
      return resolvedInodes;
    } else if ((count[0] == components.length - 2) && locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) { // if Root is the parent
      curNode[0] = acquireLockOnRoot(locks.getInodeLock(), locks);
    } else {
      curNode[0] = acquireLockOnRoot(INodeLockType.READ_COMMITTED, locks);
    }
    resolvedInodes.add(curNode[0]);
    
    while (count[0] < components.length && curNode[0] != null) {

      INodeLockType curInodeLock = null;
      if (((locks.getInodeLock() == INodeLockType.WRITE || locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) && (count[0] + 1 == components.length - 1))
              || (locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT && (count[0] + 1 == components.length - 2))) {
        curInodeLock = INodeLockType.WRITE;
      } else if (locks.getInodeLock() == INodeLockType.READ_COMMITTED) {
        curInodeLock = INodeLockType.READ_COMMITTED;
      } else {
        curInodeLock = locks.getPrecedingPathLockType();  
      }
      setINodeLockType(curInodeLock);

      lastComp = INodeUtil.getNextChild(
              curNode,
              components,
              count,
              locks.isResolveLink(),
              true);

      if (curNode[0] != null) {
        locks.addLockedINodes(curNode[0], curInodeLock);
        if (curNode[0].isSubtreeLocked() && isNameNodeAlive(curNode[0].getSubtreeLockOwner())) {
          if (!locks.getIgnoreLocalSubtreeLocks()
              || locks.getIgnoreLocalSubtreeLocks() && locks.getNamenodeId() != curNode[0].getSubtreeLockOwner()) {
            throw new SubtreeLockedException();
          }
        }
        resolvedInodes.add(curNode[0]);
      }

      if (lastComp) {
        break;
      }
    }

    // lock upgrade if the path was not fully resolved
    if(resolvedInodes.size() != components.length){ // path was not fully resolved
      INode inodeToReread = null;
      if(locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT){
        if(resolvedInodes.size() <= components.length-2){
          inodeToReread = resolvedInodes.peekLast();
        }
      }else if(locks.getInodeLock() == INodeLockType.WRITE){
        inodeToReread = resolvedInodes.peekLast();
      }

      if(inodeToReread!=null){
        INode inode = pkINodeLookUpByNameAndPid(locks.getInodeLock(), inodeToReread.getLocalName(), inodeToReread.getParentId(), locks);
        if(inode != null){ // re-read after taking write lock to make sure that no one has created the same inode. 
          locks.addLockedINodes(inode, locks.getInodeLock());
          String existingPath = buildPath(path, resolvedInodes.size());  
          LinkedList<INode> rest = acquireLockOnRestOfPath(locks.getInodeLock(), inode,
                  path, existingPath, false);
          resolvedInodes.addAll(rest);
        }
      }
    }
    return resolvedInodes;
  }

  private  INode pkINodeLookUpByNameAndPid(
          INodeLockType lock,
          String name,
          int parentId,
          HDFSTransactionLocks locks)
          throws PersistanceException {
    setINodeLockType(lock);
    INode inode = EntityManager.find(INode.Finder.ByPK_NameAndParentId, name, parentId);
    locks.addLockedINodes(inode, lock);
    return inode;
  }

  private  INode iNodeScanLookUpByID(
          INodeLockType lock,
          int id,
          HDFSTransactionLocks locks)
          throws PersistanceException {
    setINodeLockType(lock);
    INode inode = EntityManager.find(INode.Finder.ByINodeID, id);
    locks.addLockedINodes(inode, lock);
    return inode;
  }
  
  
  private  void setINodeLockType(INodeLockType lock) throws StorageException {
    switch (lock) {
      case WRITE:
      case WRITE_ON_PARENT:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITTED:
        EntityManager.readCommited();
        break;
    }
  }

  private  INode acquireLockOnRoot(INodeLockType lock, HDFSTransactionLocks locks) throws PersistanceException {
    LOG.debug("Acquring " + lock + " on the root node");
    return pkINodeLookUpByNameAndPid(lock, INodeDirectory.ROOT_NAME, INodeDirectory.ROOT_PARENT_ID, locks);
  }

    private Future readINodeAttributesAsync() throws PersistanceException {
        final String threadName = getTransactionName();
        final Long parentThreadId = Thread.currentThread().getId();
        ParallelReadThread pThread = new ParallelReadThread(Thread.currentThread().getId(), null) {
            @Override
            public void run() {
                try {
    List<HopINodeCandidatePK> pks = new ArrayList<HopINodeCandidatePK>();
    for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
      for (INode inode : resolvedINodes) {
        if (inode instanceof INodeDirectoryWithQuota) {
          HopINodeCandidatePK pk = new HopINodeCandidatePK(inode.getId());
          pks.add(pk);
        }
      }
    }
    if(!pks.isEmpty()){
                        if(isAsyncReadingEnabled){
                            EntityManager.begin();
                        }
                        concurrentAcquireLockList(LockType.READ_COMMITTED, INodeAttributes.Finder.ByPKList, parentThreadId, pks);
                        if(isAsyncReadingEnabled){
                            EntityManager.commit(locks);
                        }
                    }
                } catch (Exception ex) {
                    exceptionList.add(ex); //after join all exceptions will be thrown
                    try {
                        if(isAsyncReadingEnabled){
                            EntityManager.rollback(locks);
                        }
                    } catch (StorageException ex1) {
                        exceptionList.add(ex1);
                    }
                }
            }
        };
        if(isAsyncReadingEnabled){
            Future future = GlobalThreadPool.getExecutorService().submit(pThread);
            return future;
        }else{
            pThread.run();
            return null;
    }
  }
  
  private String getTransactionName(){
    return NDCWrapper.peek()+" Async";
  }
  
  private List<Future> startNonLockingAsyncReadThreads() throws PersistanceException, ExecutionException{
      List<Future> futures = acquireBlockRelatedInfoASync();
      Future future = readINodeAttributesAsync();
      if(future != null){
        futures.add(future);
      }
      return futures;
  }
  
//  private void setPartitioningKey(String path){
//    byte[][] components = INode.getPathComponents(path);
//    byte[] file = components[components.length-1];
//    int inodeId = INode.getPartitionKey(file);
//    LOG.debug("Setting Partitioning Key for file "+file+" toBe "+ inodeId);
//    setPartitioningKey(inodeId, false);
//    
//  }
  private void setPartitioningKey(Integer inodeId) throws StorageException {
    boolean isSetPartitionKeyEnabled = conf.getBoolean(DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED, DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED_DEFAULT);
    if (inodeId == null || !isSetPartitionKeyEnabled) {
      LOG.warn("Transaction Partition Key is not Set");
    } else {
      //set partitioning key
      Object[] key = new Object[2];
      key[0] = inodeId; //pid
      key[1] = new Long(0);

      EntityManager.setPartitionKey(BlockInfoDataAccess.class, key);
        LOG.debug("Setting Partitioning Key to be " + inodeId);
    }
  }
  
  private void setPartitioningKeyForLeader() throws StorageException{
    boolean isSetPartitionKeyEnabled = conf.getBoolean(DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED, DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED_DEFAULT);
    if (isSetPartitionKeyEnabled) {
      //set partitioning key
      Object[] key = new Object[2];
      key[0] = LeaderElection.LEADER_INITIALIZATION_ID; //pid
      key[1] = HopLeader.DEFAULT_PARTITION_VALUE;
      EntityManager.setPartitionKey(LeaderDataAccess.class, key);
      //LOG.debug("Setting Partitioning for Leader Election ");
    }
  }

  private void acquireIndividualInodeLock() throws PersistanceException {
    if (locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) {
      throw new UnsupportedOperationException("Write on parent is not supported for individual inodes.");
    }

    setINodeLockType(locks.getInodeLock());
    INode inode = EntityManager.find(INode.Finder.ByINodeID, locks.getInodeId());
    LinkedList<INode> resolvedNodes = new LinkedList<INode>();
    resolvedNodes.add(inode);
    allResolvedINodes.add(resolvedNodes);
    locks.addLockedINodes(inode, locks.getInodeLock());
  }

  private boolean isNameNodeAlive(long namenodeId) {
    if (activeNamenodes == null) {
      // We do not know yet, be conservative
      return true;
    }

    for (ActiveNamenode namenode : activeNamenodes) {
      if (namenode.getId() == namenodeId) {
        return true;
      }
    }
    return false;
  }

  public static void setActiveNamenodes(Collection<ActiveNamenode> activeNamenodes) {
    HDFSTransactionLockAcquirer.activeNamenodes = activeNamenodes;
  }
}
