package se.sics.hop.metadata.lock;

import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLocks;
import se.sics.hop.exception.INodeResolveException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INodeIdentifier;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeader;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.log4j.NDC;
import se.sics.hop.exception.AcquireLockInterruptedException;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import static se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType.READ_COMMITED;
import static se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType.WRITE_ON_PARENT;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeResolveType;
import static se.sics.hop.transaction.lock.TransactionLockTypes.LockType.READ;
import static se.sics.hop.transaction.lock.TransactionLockTypes.LockType.READ_COMMITTED;
import static se.sics.hop.transaction.lock.TransactionLockTypes.LockType.WRITE;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.transaction.lock.ParallelReadThread;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Salman <salman@sics.se>
 */
public class HDFSTransactionLockAcquirer extends TransactionLockAcquirer{

  private final static Log LOG = LogFactory.getLog(HDFSTransactionLockAcquirer.class);
  private final HDFSTransactionLocks locks;
  private LinkedList<LinkedList<INode>> allResolvedINodes = new LinkedList<LinkedList<INode>>(); //linked lists are important. we need to perserv insertion order
  private LinkedList<Lease> leaseResults = new LinkedList<Lease>();
  private LinkedList<BlockInfo> blockResults = new LinkedList<BlockInfo>();
  private boolean terminateAsyncThread = false;
  

  public HDFSTransactionLockAcquirer() {
    this.locks = new HDFSTransactionLocks();
  }

  public HDFSTransactionLockAcquirer(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    this.locks = new HDFSTransactionLocks(resolvedInodes, preTxPathFullyResolved);
  }

  public HDFSTransactionLocks getLocks() {
    return this.locks;
  }

  @Override
  public TransactionLocks acquire() throws PersistanceException, UnresolvedPathException {
    // acuires lock in order
    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      acquireInodeLocks(locks.getInodeParam());
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockParam() != null) {
        throw new StorageException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }
      blockResults.addAll(acquireBlockLock());
    }

    acquireLeaseAndLpathLockNormal();
    acquireLocksOnVariablesTable();
    readINodeAttributes();
    acquireBlockRelatedInfoASync();
    return locks;
  }

  /**
   * This method acquires lockk on the inode starting with a block-id. The
   * lock-types should be set before using add* methods. Otherwise, no lock
   * would be acquired.
   *
   * @throws PersistanceException
   */
  public HDFSTransactionLocks acquireByBlock(INodeIdentifier inodeIdentifer) throws PersistanceException, UnresolvedPathException {
//    if(id == null && pid == null && name == null && locks.getInodeResolveType() == null){
//       throw new StorageException("Unable to take locks on inode");
//    }
    INode inode = null;
    if (locks.getInodeResolveType() == INodeResolveType.PATH) {
      checkPathIsResolved();
      if (!locks.getPreTxResolvedInodes().isEmpty()) {
        inode = takeLocksFromRootToLeaf(locks.getPreTxResolvedInodes(), locks.getInodeLock());
        allResolvedINodes.add(locks.getPreTxResolvedInodes());
      }
    } 
    
//    if (inode == null && pid != null && name != null) {
//        inode = pkINodeLookUpByNameAndPid(locks.getInodeLock(), name, pid, locks);
//    }
    
    if (inode == null && inodeIdentifer != null) {
      // dangling block
      // take lock on the indeId basically bring null in the cache
      inode = iNodePruneScanLookUpByID(locks.getInodeLock(), inodeIdentifer.getInode_id(), inodeIdentifer.getPart_key(), locks);
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
      if (locks.getBlockParam() != null) {
        for (BlockInfo blk : allBlks) {
          if (blk.getBlockId() == locks.getBlockParam()) {
            found = true;
            break;
          }
        }

        if (!found) {
          acquireLock(LockType.READ_COMMITTED, BlockInfo.Finder.ById, locks.getBlockParam());
          // we need to bring null for the other tables too. so put a dummy obj in the blocksResults list
          BlockInfo blk = new BlockInfo();
          if (inode != null) {
            blk.setINodeIdNoPersistance(inode.getId());
          }
          blk.setBlockIdNoPersistance(locks.getBlockParam());
          
          blockResults.add(blk);
        }
      }
      
      // sort the blocks. it is important as the ndb returns the blocks in random order and two
      // txs trying to take locks on the blocks of a file will end up in dead lock 
      Collections.sort((List<BlockInfo>) blockResults, BlockInfo.Order.ByBlockId);
    }

    if (blockResults.isEmpty()) {
      BlockInfo block = acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, locks.getBlockParam());
      if (block != null) {
        blockResults.add(block);
      }
    }

    // read-committed block is the same as block found by inode-file so everything is fine and continue the rest.
    acquireLeaseAndLpathLockNormal();
    acquireLocksOnVariablesTable();
    readINodeAttributes();
    acquireBlockRelatedInfoASync();
    return locks;
  }

  public HDFSTransactionLocks acquireByLease(SortedSet<String> sortedPaths) throws PersistanceException, UnresolvedPathException {
    if (locks.getLeaseParam() == null) {
      return locks;
    }

    acquireInodeLocks(sortedPaths.toArray(new String[sortedPaths.size()]));

    blockResults.addAll(acquireBlockLock());

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
    readINodeAttributes();
    acquireBlockRelatedInfoASync();
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

  public HDFSTransactionLocks acquireForRename() throws PersistanceException, UnresolvedPathException {
    return acquireForRename(false);
  }

  public HDFSTransactionLocks acquireForRename(boolean allowExistingDir) throws PersistanceException, UnresolvedPathException {
    byte[][] srcComponents = INode.getPathComponents(locks.getInodeParam()[0]);
    byte[][] dstComponents = INode.getPathComponents(locks.getInodeParam()[1]);

    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      //[S] consider src = /a/b/c and dst = /d
      //during the acquire lock of src write locks will be acquired on parent of c and c
      //during the acquire lock of dst write lock on the root will be acquired but the snapshot 
      //layer will not let the request go to the db as it has already cached the root inode
      //one simple solution is that to acquire lock on the short path first
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
//        inodeResult = new INode[inodeResult1.length + inodeResult2.length + 1];
//        if (existingInode != null & !existingInode.isDirectory()) {
//          inodeResult[inodeResult.length - 1] = existingInode;
//        }
        }
      }
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockParam() != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }
      blockResults.addAll(acquireBlockLock());
    }

    acquireLeaseAndLpathLockNormal();
    acquireLocksOnVariablesTable();
    readINodeAttributes();
    acquireBlockRelatedInfoASync();
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

  private ParallelReadThread acquireReplicasLockASync(final FinderType finder, final List<Long> params) throws PersistanceException {
    final String threadName = getTransactionName();
    ParallelReadThread pThread = new ParallelReadThread(Thread.currentThread().getId()) {
      @Override
      public void run() {
        super.run(); //To change body of generated methods, choose Tools | Templates.
        try {
          NDC.push(threadName);
          if(!terminateAsyncThread){
          EntityManager.begin();
          EntityManager.readCommited();
          }
          if(params != null){
          for (Long id : params) {
              if(!terminateAsyncThread)
              acquireLockList(LockType.READ_COMMITTED, finder, id);
          } }
          else{
              if(!terminateAsyncThread)
              acquireLockList(LockType.READ_COMMITTED, finder); 
          }
          if(!terminateAsyncThread)
          EntityManager.commit(locks);
        } catch (PersistanceException ex) {
          exceptionList.add(ex); //after join all exceptions will be thrown
        } 
      }
    };
    pThread.start();
    return pThread;
  }

   List<Exception> exceptionList = new ArrayList<Exception>();
   private ParallelReadThread acquireBlockRelatedLockASync(final FinderType finder, final List<Long> params) throws PersistanceException {
     final String threadName = getTransactionName(); 
     ParallelReadThread pThread = new ParallelReadThread(Thread.currentThread().getId()) {
      @Override
      public void run() {
        super.run(); //To change body of generated methods, choose Tools | Templates.
        try {
          NDC.push(threadName);
          if(!terminateAsyncThread){
          EntityManager.begin();
          EntityManager.readCommited();
          }
          if(params != null){
          for (Long id : params) {
              if(!terminateAsyncThread)
              acquireLock(LockType.READ_COMMITTED, finder, id);
            }
          }else{
              if(!terminateAsyncThread)
              acquireLockList(LockType.READ_COMMITTED, finder); 
          }
          if(!terminateAsyncThread)
          EntityManager.commit(locks);
        } catch (PersistanceException ex) {
          exceptionList.add(ex); //after join all exceptions will be thrown
        }
      }
    };
    pThread.start();
    return pThread;
  }
  

  private LinkedList<INode> findImmediateChildren(INode lastINode) throws PersistanceException {
    LinkedList<INode> children = new LinkedList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        lockINode(locks.getInodeLock());
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
        lockINode(locks.getInodeLock());
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

    if (locks.getGenerationStampLock() != null) {
      acquireLock(locks.getGenerationStampLock(), HopVariable.Finder.GenerationStamp);
    }

    if (locks.getBlockIdCounterLock() != null) {
      acquireLock(locks.getBlockIdCounterLock(), HopVariable.Finder.BlockID);
    }
    
    if(locks.getInodeIDCounterLock() != null){
      acquireLock(locks.getInodeIDCounterLock(), HopVariable.Finder.INodeID);
    }
    
    if (locks.getStorageInfo() != null) {
      acquireLock(locks.getStorageInfo(), HopVariable.Finder.StorageInfo);
    }
    
    if (locks.getUrbLock() != null) {
      acquireLock(locks.getUrbLock(), HopVariable.Finder.ReplicationIndex);
    }
    
    if (locks.getSIdCounter() != null) {
      acquireLock(locks.getSIdCounter(), HopVariable.Finder.SIdCounter);
    }
  }
  /**
   * Acquires lock on the lease, lease-path, replicas, excess, corrupt,
   * invalidated, under-replicated and pending blocks.
   *
   * @throws PersistanceException
   */
  private void acquireBlockRelatedInfoASync() throws PersistanceException {
    // blocks related tables
    List<Thread> threads = new ArrayList<Thread>();
    if (locks.getReplicaLock() != null) {
      List<Long> params = getBlockParameters();
      threads.add(acquireReplicasLockASync(HopIndexedReplica.Finder.ByBlockId, params));
    }

    if (locks.getCrLock() != null) {
      List<Long> params = getBlockParameters();
      threads.add(acquireReplicasLockASync( HopCorruptReplica.Finder.ByBlockId, params));
    }

    if (locks.getErLock() != null) {
      List<Long> params = getBlockParameters();
      threads.add(acquireReplicasLockASync(HopExcessReplica.Finder.ByBlockId, params));
    }

    if (locks.getRucLock() != null) {
      List<Long> params = getBlockParameters();
      threads.add(acquireReplicasLockASync(ReplicaUnderConstruction.Finder.ByBlockId, params));
    }

    if (locks.getInvLocks() != null) {
      List<Long> params = getBlockParameters();
      threads.add(acquireReplicasLockASync(HopInvalidatedBlock.Finder.ByBlockId, params));
    }

    if (locks.getUrbLock() != null) {
      if(locks.isUrbLockFindAll()){
        threads.add(acquireBlockRelatedLockASync(HopUnderReplicatedBlock.Finder.All,null));
      }else{
        List<Long> params = getBlockParameters();
        threads.add(acquireBlockRelatedLockASync(HopUnderReplicatedBlock.Finder.ByBlockId, params));
      }
    }

    if (locks.getPbLock() != null) {
      List<Long> params = getBlockParameters();
      threads.add(acquireBlockRelatedLockASync(PendingBlockInfo.Finder.ByBlockId, params));
    }
    
    InterruptedException intrException = null;
    try {
      for (int i = 0; i < threads.size(); i++) {
        Thread t = threads.get(i);
        t.join();
      }
    } catch (InterruptedException e) {
      terminateAsyncThread = true;
      if(intrException == null){
        intrException = e;
      }
    }
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
      throw (PersistanceException)exceptionList.get(0);
    }
  }
  
  private List<Long> getBlockParameters() {
    List<Long> params = new ArrayList<Long>();
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        params.add(b.getBlockId());
      }
    } else // if blockResults is null then we can safely bring null in to cache
    {
      if (locks.getBlockParam() != null) {
        params.add(locks.getBlockParam()/*id*/);
      }
    }
    return params;
  }

  private void acquireInodeLocks(String... params) throws UnresolvedPathException, PersistanceException {
    switch (locks.getInodeResolveType()) {
      case PATH: // Only use memcached for this case.
      case PATH_AND_IMMEDIATE_CHILDREN: // Memcached not applicable for delete of a dir (and its children)
      case PATH_AND_ALL_CHILDREN_RECURESIVELY:
        for (int i = 0; i < params.length; i++) {
          LinkedList<INode> resolvedInodes = acquireInodeLockByPath(locks, params[i]);
          if (resolvedInodes.size() > 0) {
            INode lastINode = resolvedInodes.peekLast();
            if (locks.getInodeResolveType() == INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
              resolvedInodes.addAll(findImmediateChildren(lastINode));
            } else if (locks.getInodeResolveType() == INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY) {
              resolvedInodes.addAll(findChildrenRecursively(lastINode));
            }
          }
          allResolvedINodes.add(resolvedInodes);
        }
        break;
      // e.g. mkdir -d /opt/long/path which creates subdirs.
      // That is, the HEAD and some ancestor inodes might not exist yet.
      case PATH_WITH_UNKNOWN_HEAD: // Can try and use memcached for this case.
        for (int i = 0; i < params.length; i++) {
          String fullPath = params[i];
          checkPathIsResolved();
          int resolvedSize = locks.getPreTxResolvedInodes().size();
          String existingPath = buildPath(fullPath, resolvedSize);
          acquireInodeLocksByPreTxResolvedIDs(locks);
          INode baseDir = locks.getPreTxResolvedInodes().peekLast();
          LinkedList<INode> rest = acquireLockOnRestOfPath(locks.getInodeLock(), baseDir,
                  fullPath, existingPath, locks.isResolveLink());
          locks.getPreTxResolvedInodes().addAll(rest);
          allResolvedINodes.add(locks.getPreTxResolvedInodes());
        }
        break;

      default:
        throw new IllegalArgumentException("Unknown type " + locks.getInodeLock().name());
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

    if (locks.getBlockParam() != null) {
      long bid = (Long) locks.getBlockParam();

      BlockInfo result = acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, bid);
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

  private INode takeLocksFromRootToLeaf(LinkedList<INode> inodes, INodeLockType inodeLock) throws PersistanceException {
    LOG.debug("Taking lock on preresolved path. Path Components are "+inodes.size());
    StringBuilder msg = new StringBuilder();
    msg.append("Took Lock on the entire path ");
    INode lockedLeafINode = null;
    for (int i = 0; i < inodes.size(); i++) {
      if (i == (inodes.size() - 1)) // take specified lock
      {
        lockedLeafINode = pkINodeLookUpByNameAndPid(inodeLock, inodes.get(i).getLocalName(),inodes.get(i).getParentId(), locks);
      } else // take read commited lock
      {
        lockedLeafINode = pkINodeLookUpByNameAndPid(INodeLockType.READ_COMMITED, inodes.get(i).getLocalName(), inodes.get(i).getParentId(), locks);
      }

      if (!lockedLeafINode.getLocalName().equals("")) {
        msg.append("/");
        msg.append(lockedLeafINode.getLocalName());
      }
    }
    LOG.debug(msg.toString());
    return lockedLeafINode;
  }

  //TransacationLockAcquirer Code
  private static <T> Collection<T> acquireLockList(LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return EntityManager.findList(finder);
    } else {
      return EntityManager.findList(finder, param);
    }
  }

  private static <T> T acquireLock(LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return null;
    }
    return EntityManager.find(finder, param);
  }

  private static LinkedList<INode> acquireLockOnRestOfPath(INodeLockType lock, INode baseInode,
          String fullPath, String prefix, boolean resolveLink) throws PersistanceException, UnresolvedPathException {
    LinkedList<INode> resolved = new LinkedList<INode>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    int[] count = new int[]{prefixComps.length - 1};
    boolean lastComp;
    lockINode(lock);
    INode[] curInode = new INode[]{baseInode};
    while (count[0] < fullComps.length && curInode[0] != null) {
      lastComp = INodeUtil.getNextChild(
              curInode,
              fullComps,
              count,
              resolved,
              resolveLink,
              true);
      if (lastComp) {
        break;
      }
    }

    return resolved;
  }

  private static LinkedList<INode> acquireInodeLockByPath(HDFSTransactionLocks locks, String path) throws UnresolvedPathException, PersistanceException {
    LinkedList<INode> resolvedInodes = new LinkedList<INode>();

    if (path == null) {
      return resolvedInodes;
    }

    byte[][] components = INode.getPathComponents(path);
    INode[] curNode = new INode[1];

    int[] count = new int[]{0};
    boolean lastComp = (count[0] == components.length - 1);
    if (lastComp) // if root is the last directory, we should acquire the write lock over the root
    {
      resolvedInodes.add(acquireLockOnRoot(locks.getInodeLock(), locks));
      return resolvedInodes;
    } else if ((count[0] == components.length - 2) && locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) // if Root is the parent
    {
      curNode[0] = acquireLockOnRoot(locks.getInodeLock(), locks);
    } else {
      curNode[0] = acquireLockOnRoot(INodeLockType.READ_COMMITED, locks);
    }
    resolvedInodes.add(curNode[0]);
    
    while (count[0] < components.length && curNode[0] != null) {

      INodeLockType curInodeLock = null;
      // TODO - memcached - primary key lookup for the row.
      if (((locks.getInodeLock() == INodeLockType.WRITE || locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) && (count[0] + 1 == components.length - 1))
              || (locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT && (count[0] + 1 == components.length - 2))) {
        EntityManager.writeLock(); // if the next p-component is the last one or is the parent (in case of write on parent), acquire the write lock
        curInodeLock = INodeLockType.WRITE;
      } else if (locks.getInodeLock() == INodeLockType.READ_COMMITED) {
        EntityManager.readCommited();
        curInodeLock = INodeLockType.READ_COMMITED;
      } else {
        EntityManager.readLock();
        curInodeLock = INodeLockType.READ;
      }

      lastComp = INodeUtil.getNextChild(
              curNode,
              components,
              count,
              resolvedInodes,
              locks.isResolveLink(),
              true);
      if (curNode[0] != null) {
        locks.addLockedINodes(curNode[0], curInodeLock);
      }
      if (lastComp) {
        break;
      }
    }

    // TODO - put invalidated cache values in memcached.

    return resolvedInodes;
  }

//  private static INode pruneScanINodeById(INodeLockType lock, long id, HDFSTransactionLocks locks) throws PersistanceException {
//    lockINode(lock);
//    INode inode = EntityManager.find(INode.Finder.ByINodeID, id);
//    locks.addLockedINodes(inode, lock);
//    return inode;
//  }

  private static INode pkINodeLookUpByNameAndPid(
          INodeLockType lock,
          String name,
          int parentId,
          HDFSTransactionLocks locks)
          throws PersistanceException {
    lockINode(lock);
    INode inode = EntityManager.find(INode.Finder.ByPK_NameAndParentId, name, parentId, INode.getPartitionKey(name));
    locks.addLockedINodes(inode, lock);
    return inode;
  }

  private static INode iNodePruneScanLookUpByID(
          INodeLockType lock,
          int id,
          int partKey,
          HDFSTransactionLocks locks)
          throws PersistanceException {
    lockINode(lock);
    INode inode = EntityManager.find(INode.Finder.ByINodeID, id, partKey);
    locks.addLockedINodes(inode, lock);
    return inode;
  }
  
  
  private static void lockINode(INodeLockType lock) {
    switch (lock) {
      case WRITE:
      case WRITE_ON_PARENT:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITED:
        EntityManager.readCommited();
        break;
    }
  }

  private static INode acquireLockOnRoot(INodeLockType lock, HDFSTransactionLocks locks) throws PersistanceException {
    LOG.debug("Acquring " + lock + " on the root node");
    return pkINodeLookUpByNameAndPid(lock, INodeDirectory.ROOT_NAME, INodeDirectory.ROOT_PARENT_ID, locks);
  }
  
  //if path is already resolved then take locks based on primarny keys
  private static void acquireInodeLocksByPreTxResolvedIDs(HDFSTransactionLocks locks) throws PersistanceException {
    LinkedList<INode> resolvedInodes = locks.getPreTxResolvedInodes();
    int palthLength = resolvedInodes.size();
    int count = 0;
    boolean lastComp = (count == palthLength - 1);

    if (lastComp) { // if root is the last directory, we should acquire the write lock over the root
      acquireLockOnRoot(locks.getInodeLock(), locks);
      return;
    }

    boolean canTakeParentLock = locks.isPreTxPathFullyResolved(); //if the path is not fully resolved then there is no point in taking strong lock on the penultimate inode
    while (count < palthLength) {
      if ( // take write lock on the element if needed
              ((count == (palthLength - 1)) && (locks.getInodeLock() == INodeLockType.WRITE || locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT))
              || ((count == (palthLength - 2)) && (locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) && canTakeParentLock)) {
        pkINodeLookUpByNameAndPid(INodeLockType.WRITE, resolvedInodes.get(count).getLocalName(), resolvedInodes.get(count).getParentId(), locks);
      } else if (locks.getInodeLock() == INodeLockType.READ_COMMITED) {
        pkINodeLookUpByNameAndPid(INodeLockType.READ_COMMITED, resolvedInodes.get(count).getLocalName(), resolvedInodes.get(count).getParentId(), locks);
      } else {
        pkINodeLookUpByNameAndPid(INodeLockType.READ, resolvedInodes.get(count).getLocalName(), resolvedInodes.get(count).getParentId(), locks);
      }

      lastComp = (count == (palthLength - 1));
      count++;
      if (lastComp) {
        break;
      }
    }
  }

  private void readINodeAttributes() throws PersistanceException {
    List<Integer> ids = new ArrayList<Integer>();
    for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
      for (INode inode : resolvedINodes) {
        if (inode instanceof INodeDirectoryWithQuota) {
          ids.add(inode.getId());
        }
      }
    }
    if(!ids.isEmpty()){
      acquireLockList(LockType.READ_COMMITTED, INodeAttributes.Finder.ByPKList, ids);
    }
  }
  
  private String getTransactionName(){
    return NDC.peek()+" Async";
  }
}
