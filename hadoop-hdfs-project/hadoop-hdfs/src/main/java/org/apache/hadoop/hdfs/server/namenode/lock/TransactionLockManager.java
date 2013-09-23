package org.apache.hadoop.hdfs.server.namenode.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
//import org.apache.hadoop.hdfs.server.blockmanagement.GenerationStamp;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
//import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
//import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.Leader;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockTypes.INodeLockType;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockTypes.LockType;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockTypes.INodeResolveType;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TransactionLockManager {

  private final static Log LOG = LogFactory.getLog(TransactionLockManager.class);
  
  private final TransactionLocks locks;

  private INode[] inodeResult = null;
  private Collection<Lease> leaseResults = null;
  private Collection<BlockInfo> blockResults = null;
 
  

  public TransactionLockManager(TransactionLocks locks) {
      this.locks = locks;
  }

  

  private List<Lease> acquireLeaseLock() throws PersistanceException {

    checkStringParam(locks.getLeaseParam());
    SortedSet<String> holders = new TreeSet<String>();
    if (locks.getLeaseParam() != null) {
      holders.add((String) locks.getLeaseParam());
    }

    if (inodeResult != null) {
      for (INode f : inodeResult) {
        if (f instanceof INodeFileUnderConstruction) {
          holders.add(((INodeFileUnderConstruction) f).getClientName());
        }
      }
    }

    List<Lease> leases = new ArrayList<Lease>();
    for (String h : holders) {
      Lease lease = TransactionLockAcquirer.acquireLock(locks.getLeaseLock(), Lease.Finder.ByPKey, h);
      if (lease != null) {
        leases.add(lease);
      }
    }

    return leases;
  }

  /**
   * Acquires lock on lease path and lease having leasepath. This is used by the
   * test cases.
   *
   * @param leasePath
   */
  public void acquireByLeasePath(String leasePath, TransactionLockTypes.LockType leasePathLock, TransactionLockTypes.LockType leaseLock) throws PersistanceException {
    LeasePath lp = TransactionLockAcquirer.acquireLock(leasePathLock, LeasePath.Finder.ByPKey, leasePath);
    if (lp != null) {
      TransactionLockAcquirer.acquireLock(leaseLock, Lease.Finder.ByHolderId, lp.getHolderId());
    }
  }

  private void checkStringParam(Object param) {
    if (param != null && !(param instanceof String)) {
      throw new IllegalArgumentException("Param is expected to be a String but is " + param.getClass().getName());
    }
  }

  private List<LeasePath> acquireLeasePathsLock() throws PersistanceException {
    List<LeasePath> lPaths = new LinkedList<LeasePath>();
    if (leaseResults != null) {
      for (Lease l : leaseResults) {
        Collection<LeasePath> result = TransactionLockAcquirer.acquireLockList(locks.getLpLock(), LeasePath.Finder.ByHolderId, l.getHolderID());
        if (!l.getHolder().equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) { // We don't need to keep the lps result for namenode-lease. 
          lPaths.addAll(result);
        }
      }
    }

    return lPaths;
  }


  private void acquireReplicasLock(LockType lock, FinderType finder) throws PersistanceException {
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        TransactionLockAcquirer.acquireLockList(lock, finder, b.getBlockId());
      }
    } else // if blockResults is null then we can safely bring null in to cache
    {
      if (locks.getBlockParam() != null) {
        TransactionLockAcquirer.acquireLockList(lock, finder, locks.getBlockParam()/*id*/);
      }
    }
  }

  private void acquireBlockRelatedLock(LockType lock, FinderType finder) throws PersistanceException {
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        TransactionLockAcquirer.acquireLock(lock, finder, b.getBlockId());
      }
    } else // if blockResults is null then we can safely bring null in to cache
    {
        if(locks.getBlockParam() != null){
            TransactionLockAcquirer.acquireLock(lock, finder, locks.getBlockParam()/*id*/);
        }
    }
  }

  private INode[] findImmediateChildren(INode[] inodes) throws PersistanceException {
    ArrayList<INode> children = new ArrayList<INode>();
    if (inodes != null) {
      for (INode dir : inodes) {
        if (dir instanceof INodeDirectory) {
          children.addAll(((INodeDirectory) dir).getChildren());
        } else {
          // immediate children of  INodeFile is the inode itself.
          children.add(dir);
        }
      }
    }
    //if(child)
    inodes = new INode[children.size()];
    return children.toArray(inodes);
  }

  private INode[] findChildrenRecursively(INode[] inodes) throws PersistanceException {
    ArrayList<INode> children = new ArrayList<INode>();
    LinkedList<INode> unCheckedDirs = new LinkedList<INode>();
    if (inodes != null) {
      for (INode inode : inodes) {
        if (inode instanceof INodeDirectory) {
          unCheckedDirs.add(inode);
        } else {
          children.add(inode);
        }
      }
    }

    // Find all the children in the sub-directories.
    while (!unCheckedDirs.isEmpty()) {
      INode next = unCheckedDirs.poll();
      if (next instanceof INodeDirectory) {
        unCheckedDirs.addAll(((INodeDirectory) next).getChildren());
      } else if (next instanceof INodeFile) {
        children.add(next); // We only need INodeFiles later to get the blocks.
      }
    }

    inodes = new INode[children.size()];
    return children.toArray(inodes);
  }

    private String buildPath(String path, int size) {
        StringBuilder builder = new StringBuilder();
        byte[][] components = INode.getPathComponents(path);

        for (int i = 0; i < Math.min(components.length, size); i++) {
            if (i == 0) {
                builder.append("/");
            } else {
                if(i != 1 ){
                builder.append("/");
                }
                builder.append(DFSUtil.bytes2String(components[i]));
            }
        }

    return builder.toString();
  }

  private Lease acquireNameNodeLease() throws PersistanceException {
    if (locks.getNnLeaseLock() != null) {
      return TransactionLockAcquirer.acquireLock(locks.getNnLeaseLock(), Lease.Finder.ByPKey, HdfsServerConstants.NAMENODE_LEASE_HOLDER);
    }
    return null;
  }

  public void acquire() throws PersistanceException, UnresolvedPathException {
    // acuires lock in order
    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      inodeResult = acquireInodeLocks(locks.getInodeResolveType(), locks.getInodeLock(), locks.getInodeParam());
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockParam() != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }

      blockResults = acquireBlockLock(locks.getBlockLock(), locks.getBlockParam());
    }

    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
    acquireLeaderLock();
  }

  public void acquireForRename() throws PersistanceException, UnresolvedPathException {
    acquireForRename(false);
  }

  public void acquireForRename(boolean allowExistingDir) throws PersistanceException, UnresolvedPathException {
    // TODO[H]: Sort before taking locks.
    // acuires lock in order
    String src = locks.getInodeParam()[0];
    String dst = locks.getInodeParam()[1];
    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      INode[] inodeResult1 = acquireInodeLocks(locks.getInodeResolveType(), locks.getInodeLock(), src);
      INode[] inodeResult2 = acquireInodeLocks(locks.getInodeResolveType(), locks.getInodeLock(), dst);
      if (allowExistingDir) // In deprecated rename, it allows to move a dir to an existing destination.
      {
        LinkedList<INode> dstINodes = TransactionLockAcquirer.acquireInodeLockByPath(locks.getInodeLock(), dst, locks.isResolveLink()); // reads from snapshot.
        byte[][] dstComponents = INode.getPathComponents(dst);
        byte[][] srcComponents = INode.getPathComponents(src);
        if (dstINodes.size() == dstComponents.length - 1 && dstINodes.getLast().isDirectory()) {
          //the dst exist and is a directory.
          INode existingInode = TransactionLockAcquirer.acquireINodeLockByNameAndParentId(
                  locks.getInodeLock(),
                  DFSUtil.bytes2String(srcComponents[srcComponents.length - 1]),
                  dstINodes.getLast().getId());
//        inodeResult = new INode[inodeResult1.length + inodeResult2.length + 1];
//        if (existingInode != null & !existingInode.isDirectory()) {
//          inodeResult[inodeResult.length - 1] = existingInode;
//        }
        }
      }
      inodeResult = new INode[inodeResult1.length + inodeResult2.length];
      System.arraycopy(inodeResult1, 0, inodeResult, 0, inodeResult1.length);
      System.arraycopy(inodeResult2, 0, inodeResult, inodeResult1.length, inodeResult2.length);
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockParam() != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }

      blockResults = acquireBlockLock(locks.getBlockLock(), locks.getBlockParam());
    }

    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
    acquireLeaderLock();
  }

  private void acquireLeaderLock() throws PersistanceException {
    if (locks.getLeaderLock() != null) {
      if (locks.getLeaderIds().length == 0) {
        TransactionLockAcquirer.acquireLockList(locks.getLeaderLock(), Leader.Finder.All);
      } else {
        for (long id : locks.getLeaderIds()) {
          TransactionLockAcquirer.acquireLock(
                  locks.getLeaderLock(),
                  Leader.Finder.ById,
                  id,
                  Leader.DEFAULT_PARTITION_VALUE);
        }
      }
    }
  }

  private void acquireLeaseAndLpathLockNormal() throws PersistanceException {
    if (locks.getLeaseLock() != null) {
      leaseResults = acquireLeaseLock();
    }

    if (locks.getLpLock() != null) {
      acquireLeasePathsLock();
    }
  }

  /**
   * Acquires lock on the lease, lease-path, replicas, excess, corrupt,
   * invalidated, under-replicated and pending blocks.
   *
   * @throws PersistanceException
   */
  private void acquireBlockRelatedLocksNormal() throws PersistanceException {

//HOP if (blockResults != null && !blockResults.isEmpty()) //[S] commented this to bring null in to the cache for invalid/deleted blocks
//    {
    if (locks.getReplicaLock() != null) {
      acquireReplicasLock(locks.getReplicaLock(), IndexedReplica.Finder.ByBlockId);
    }

    if (locks.getCrLock() != null) {
      acquireReplicasLock(locks.getCrLock(), CorruptReplica.Finder.ByBlockId);
    }

    if (locks.getErLock() != null) {
      acquireReplicasLock(locks.getErLock(), ExcessReplica.Finder.ByBlockId);
    }

    if (locks.getRucLock() != null) {
      acquireReplicasLock(locks.getRucLock(), ReplicaUnderConstruction.Finder.ByBlockId);
    }

    if (locks.getInvLocks() != null) {
      acquireReplicasLock(locks.getInvLocks(), InvalidatedBlock.Finder.ByBlockId);
    }

//      if (urbLock != null) {
//        acquireBlockRelatedLock(urbLock, UnderReplicatedBlock.Finder.ByBlockId);
//      }

//      if (pbLock != null) {
//        acquireBlockRelatedLock(pbLock, PendingBlockInfo.Finder.ByPKey);
//      }
//    }

//    if (blockKeyLock != null) {
//      if (blockKeyIds != null) {
//        for (int id : blockKeyIds) {
//          TransactionLockAcquirer.acquireLock(blockKeyLock, BlockKey.Finder.ById, id);
//        }
//      }
//      if (blockKeyTypes != null) {
//        for (short type : blockKeyTypes) {
//          TransactionLockAcquirer.acquireLock(blockKeyLock, BlockKey.Finder.ByType, type);
//        }
//      }
//    }
//
//    if (generationStampLock != null) {
//      TransactionLockAcquirer.acquireLock(generationStampLock, GenerationStamp.Finder.Counter);
//    }
  }

  private INode[] acquireInodeLocks(INodeResolveType resType, INodeLockType lock, String... params) throws UnresolvedPathException, PersistanceException {
    INode[] inodes = new INode[params.length];
    switch (resType) {
      case PATH: // Only use memcached for this case.
      case PATH_AND_IMMEDIATE_CHILDREN: // Memcached not applicable for delete of a dir (and its children)
      case PATH_AND_ALL_CHILDREN_RECURESIVELY:
        for (int i = 0; i < params.length; i++) {
          // TODO - MemcacheD Lookup of path
          // On
          LinkedList<INode> resolvedInodes =
                  TransactionLockAcquirer.acquireInodeLockByPath(lock, params[i], locks.isResolveLink());
          if (resolvedInodes.size() > 0) {
            inodes[i] = resolvedInodes.peekLast();
          }
        }
        if (resType == INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
          inodes = findImmediateChildren(inodes);
        } else if (resType == INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY) {
          inodes = findChildrenRecursively(inodes);
        }
        break;
      // e.g. mkdir -d /opt/long/path which creates subdirs.
      // That is, the HEAD and some ancestor inodes might not exist yet.
      case PATH_WITH_UNKNOWN_HEAD: // Can try and use memcached for this case.
        for (int i = 0; i < params.length; i++) {
          // TODO Test this in all different possible scenarios
          String fullPath = params[i];
          checkPathIsResolved();
          int resolvedSize = locks.getResolvedInodes().size();
          String existingPath = buildPath(fullPath, resolvedSize);
          TransactionLockAcquirer.acquireInodeLockByResolvedPath(lock, locks.getResolvedInodes());
          INode baseDir = locks.getResolvedInodes().peekLast();
          LinkedList<INode> rest = TransactionLockAcquirer.acquireLockOnRestOfPath(lock, baseDir,
                  fullPath, existingPath, locks.isResolveLink());
          locks.getResolvedInodes().addAll(rest);
          inodes[i] = locks.getResolvedInodes().peekLast();
        }
        break;

      default:
        throw new IllegalArgumentException("Unknown type " + lock.name());
    }

    return inodes;
  }
  
  private void checkPathIsResolved() throws INodeResolveException {
    if (locks.getResolvedInodes() == null) {
      throw new INodeResolveException(String.format(
              "Requires to have inode-id(s) in order to do this operation. "
              + "ResolvedInodes is null."));
    }
  }

  private List<BlockInfo> acquireBlockLock(LockType lock, Long param) throws PersistanceException {

    List<BlockInfo> blocks = new ArrayList<BlockInfo>();

    if (locks.getBlockParam() != null) {
      long bid = (Long) param;

      BlockInfo result = TransactionLockAcquirer.acquireLock(lock, BlockInfo.Finder.ById, bid);
      if (result != null) {
        blocks.add(result);
      }
    } else if (inodeResult != null) {
      for (INode inode : inodeResult) {
        if (inode instanceof INodeFile) {
          blocks.addAll(TransactionLockAcquirer.acquireLockList(lock, BlockInfo.Finder.ByInodeId, inode.getId()));
        }
      }
    }

    return blocks;
  }

  /**
   * This method acquires lockk on the inode starting with a block-id. The
   * lock-types should be set before using add* methods. Otherwise, no lock
   * would be acquired.
   *
   * @throws PersistanceException
   */
  public void acquireByBlock(long inodeId) throws PersistanceException, UnresolvedPathException {
    if (locks.getInodeLock() == null || locks.getBlockParam() == null) {// inodelock must be set before.
      return;
    }
    INode inode = null; 
    if (locks.getInodeResolveType() == INodeResolveType.PATH) {
      checkPathIsResolved();
      if (!locks.getResolvedInodes().isEmpty()) {
        inode = takeLocksFromRootToLeaf(locks.getResolvedInodes(), locks.getInodeLock());
      }
    }else{
        inode = TransactionLockAcquirer.acquireINodeLockById(locks.getInodeLock(), inodeId);
    }

    if (inode != null) {
      if (!(inode instanceof INodeFile)) {
        return; //it should abort the transaction and retry at this stage. Cause something is changed in the storage.
      }
      inodeResult = new INode[1];
      inodeResult[0] = inode;
    }

    blockResults = TransactionLockAcquirer.acquireLockList(locks.getBlockLock(), BlockInfo.Finder.ByInodeId, inodeId);

    if (blockResults.isEmpty()) {
        BlockInfo block = TransactionLockAcquirer.acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, locks.getBlockParam());
      if (block != null) {
        blockResults.add(block);
      }
    }

    // read-committed block is the same as block found by inode-file so everything is fine and continue the rest.
    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
  }

  private INode takeLocksFromRootToLeaf(LinkedList<INode> inodes, INodeLockType inodeLock) throws PersistanceException {

    StringBuilder msg = new StringBuilder();
    msg.append("Took Lock on the entire path ");
    INode lockedLeafINode = null;
    for (int i = 0; i < inodes.size(); i++) {
      if (i == (inodes.size() - 1)) // take specified lock
      {
        lockedLeafINode = TransactionLockAcquirer.acquireINodeLockById(inodeLock, inodes.get(i).getId());
      } else // take read commited lock
      {
        lockedLeafINode = TransactionLockAcquirer.acquireINodeLockById(INodeLockType.READ_COMMITED, inodes.get(i).getId());
      }

      if (!lockedLeafINode.getLocalName().equals("")) {
        msg.append("/");
        msg.append(lockedLeafINode.getLocalName());
      }
    }
    LOG.debug(msg.toString());
    return lockedLeafINode;
  }

  public void acquireByLease(SortedSet<String> sortedPaths) throws PersistanceException, UnresolvedPathException {
    if (locks.getLeaseParam() == null) {
      return;
    }

    inodeResult = acquireInodeLocks(INodeResolveType.PATH, locks.getInodeLock(), sortedPaths.toArray(new String[sortedPaths.size()]));

    if (inodeResult.length == 0) {
      return; // TODO: something is wrong, it should retry again.
    }
    leaseResults = new ArrayList<Lease>();
    Lease nnLease = acquireNameNodeLease(); // NameNode lease is always acquired first.
    if (nnLease != null) {
      leaseResults.add(nnLease);
    }
    Lease lease = TransactionLockAcquirer.acquireLock(locks.getLeaseLock(), Lease.Finder.ByPKey, locks.getLeaseParam());
    if (lease == null) {
      return; // Lease does not exist anymore.
    }
    leaseResults.add(lease);
    blockResults = acquireBlockLock(locks.getBlockLock(), null);

    List<LeasePath> lpResults = acquireLeasePathsLock();
    if (lpResults.size() > sortedPaths.size()) {
      return; // TODO: It should retry again, cause there are new lease-paths for this lease which we have not acquired their inodes locks.
    }

    acquireBlockRelatedLocksNormal();
  }
}
